import mqtt_as
import uasyncio
import _thread
import ujson
import uio
import machine
import random
import btree
import struct

class ServerRegister:

    def __init__(self, name, meta, value=None):
        self.name = name
        self.meta = meta
        self.value = value
        self.registry = None

    def get_name(self):
        return self.name

    def get_meta(self):
        return self.meta

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value

    def set_value_local(self, value):
        if self.value != value:
            self.value = value
            if self.registry is not None:
                self.registry.publish_register_value(self.name)

class ServerReadOnlyRegister(ServerRegister):

    def __init__(self, name, meta, value=None):
        super().__init__(name, meta, value)

    def set_value(self, value):
        raise Exception("Cannot set value of read-only register")

__default_db = None

def __get_default_db():
    global __default_db
    if __default_db is None:
        try:
            db_file = open("/regs.btree", "r+b")
        except OSError:
            db_file = open("/regs.btree", "w+b")

        __default_db = btree.open(db_file, pagesize=512, cachesize=512)
    return __default_db

class PersistentServerRegister(ServerRegister):

    def __init__(self, name, meta, default, db = None):
        super().__init__(name, meta)
        self.db = db or __get_default_db()

        if self.name not in self.db:
            self.set_value(default)

    def get_value(self):
        return self.from_bytes(self.db.get(self.name))

    def set_value(self, value):
        self.db[self.name] = self.to_bytes(value)
        self.db.flush()


class BooleanPersistentServerRegister(PersistentServerRegister):

    def __init__(self, name, meta, default=False, db = None):
        super().__init__(name, meta, default, db)

    def to_bytes(self, value):
        return b'\1' if value == True else b'\0'

    def from_bytes(self, value):
        return True if value == b'\1' else False

class FloatPersistentServerRegister(PersistentServerRegister):

    def __init__(self, name, meta, default=False, db = None):
        super().__init__(name, meta, default, db)

    def to_bytes(self, value):
        return struct.pack('f', value)

    def from_bytes(self, value):
        return struct.unpack('f', value)[0]

class ServerListHandler:

    def __init__(self, registry, registers=[]):
        self.registers = {}
        for register in registers:
            self.registers[register.get_name()] = register
            register.registry = registry

    def get_names(self):
        return list(self.registers.keys())

    def get_meta(self, name):
        return self.registers[name].get_meta()

    def get_value(self, name):
        return self.registers[name].get_value()

    def set_value(self, name, value):
        self.registers[name].set_value(value)

class ClientRegister:
    def __init__(self, name):
        self.name = name
        self.value = None

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value

class ClientListHandler:

    def __init__(self, registers=[]):
        self.registers = {}
        for register in registers:
            name = register.get_name()
            self.registers[name] = register

    def get_names(self):
        return list(self.registers.keys())

    def set_value(self, name, value):
        self.registers[name].set_value(value)

class Registry:

    advertise_in_progress = False

    def __init__(self, wifi_ssid, wifi_password, mqtt_broker, server=[], client=[], online_cb=None, debug=False):
        self.debug = debug

        self.server_handler = ServerListHandler(
            self, server) if type(server) is list else server
        self.client_handler = ClientListHandler(
            client) if type(client) is list else client

        self.server_names = self.server_handler.get_names()
        self.client_names = self.client_handler.get_names()

        self.client_timeouts = {}
        self.publish_in_progress = {}

        mqtt_config = mqtt_as.config.copy()
        mqtt_config['ssid'] = wifi_ssid
        mqtt_config['wifi_pw'] = wifi_password
        mqtt_config['server'] = mqtt_broker
        mqtt_config['queue_len'] = 32

        mqtt_as.MQTTClient.DEBUG = debug
        self.mqtt_client = mqtt_as.MQTTClient(mqtt_config)

        self.online_cb = online_cb if online_cb is not None else lambda online: None

        self.online_cb(False)

    async def __publish_json(self, topic, val):
        message = uio.BytesIO()

        if val != None:
            ujson.dump(val, message)

        message = message.getvalue()

        await self.mqtt_client.publish(topic, message, qos=1)

    def publish_register_value(self, name):

        async def do_async():

            value = self.server_handler.get_value(name)

            if self.debug:
                print('Publishing register value:', name, value)

            try:
                await self.__publish_json('register/'+name+'/is', value)
                if self.debug:
                    print('OK, published register value:', name, value)

            except Exception as e:
                print('Error publishing register value: ', e)
            finally:
                pubs = self.publish_in_progress[name]
                if pubs > 0:
                    self.publish_in_progress[name] =  pubs - 1

        if name not in self.publish_in_progress:
            self.publish_in_progress[name] = 0

        if self.publish_in_progress[name] < 2:
            self.publish_in_progress[name] = self.publish_in_progress[name] + 1
            uasyncio.create_task(do_async())
        else:
            if self.debug:
                print('Publish in progress, skipping')

    def advertise_registers(self):

        async def do_async():

            self.advertise_in_progress = True
            try:

                if self.debug:
                    print('Advertising registers')

                for name in self.server_names:
                    meta = self.server_handler.get_meta(name)
                    await self.__publish_json('register/'+name+'/advertise', meta)

            finally:
                self.advertise_in_progress = False

        if not self.advertise_in_progress:
            uasyncio.create_task(do_async())

    def reset_client_timeout(self, name, first=False):

        async def do_async(first):
            while True:
                try:
                    if not first:
                        await uasyncio.sleep_ms(random.randint(8000, 12000))

                    first = False

                    if self.debug:
                        print('Forcing get for client register', name)

                    await self.mqtt_client.publish('register/'+name+'/get', '', qos=1)
                    await uasyncio.sleep_ms(10000)

                    if self.debug:
                        print('Timeout for client register', name)

                    self.client_handler.set_value(name, None)

                except Exception as e:
                    if self.debug:
                        print('Error in reset_client_timeout:', e)

        if name in self.client_timeouts:
            self.client_timeouts[name].cancel()

        self.client_timeouts[name] = uasyncio.create_task(do_async(first))

    async def run_async(self):

        self.online_cb(True)
        await uasyncio.sleep_ms(200)
        self.online_cb(False)

        while True:
            try:
                await self.mqtt_client.connect()
                break
            except Exception as e:
                if self.debug:
                    print('Error connecting to MQTT broker:', e)

                await uasyncio.sleep_ms(5000)
                if str(e) == 'Wifi Internal Error':
                    machine.reset()

        async def up_event_loop():
            while True:
                try:
                    await self.mqtt_client.up.wait()
                    self.mqtt_client.up.clear()
                    self.online_cb(True)

                    async def subscribe(topic):
                        if self.debug:
                            print('Subscribing to:', topic)

                        await self.mqtt_client.subscribe(topic, qos=1)

                    if len(self.server_names) > 0:
                        await subscribe('register/advertise!')

                    for name in self.server_names:
                        await subscribe('register/'+name+'/get')
                        await subscribe('register/'+name+'/set')

                    for name in self.client_names:
                        await subscribe('register/'+name+'/is')
                        self.reset_client_timeout(name, first=True)

                    self.advertise_registers()

                except Exception as e:
                    if self.debug:
                        print('Error in up_event_loop:', e)


        uasyncio.create_task(up_event_loop())

        async def down_event_loop():
            while True:
                try:
                    await self.mqtt_client.down.wait()
                    self.mqtt_client.down.clear()
                    self.online_cb(False)
                except Exception as e:
                    if self.debug:
                        print('Error in down_event_loop:', e)

        uasyncio.create_task(down_event_loop())

        async def read_messages():
            async for topic, message, retained in self.mqtt_client.queue:
                if not retained:
                    try:
                        topic = topic.decode()

                        if topic == 'register/advertise!':
                            self.advertise_registers()

                        else:

                            if topic.startswith('register/'):

                                if topic.endswith('/get'):
                                    name = topic[9:-4]
                                    if self.debug:
                                        print('Get', name)
                                    self.publish_register_value(name)

                                else:
                                    if topic.endswith('/set'):
                                        name = topic[9:-4]
                                        value = None if len(message) == 0 else ujson.load(
                                            uio.BytesIO(message.decode()))
                                        if self.debug:
                                            print('Set', name, value)
                                        self.server_handler.set_value(
                                            name, value)
                                        self.publish_register_value(name)

                                    else:
                                        if topic.endswith('/is'):
                                            name = topic[9:-3]
                                            value = None if len(message) == 0 else ujson.load(
                                                uio.BytesIO(message.decode()))
                                            if self.debug:
                                                print(name, 'is', value)
                                            self.reset_client_timeout(name)
                                            self.client_handler.set_value(
                                                name, value)

                    except Exception as e:
                        if self.debug:
                            print('Error handling message because:', e)

        await read_messages()

    def start(self, background=False):
        if background:
            _thread.stack_size(32768)
            _thread.start_new_thread(lambda: uasyncio.run(self.run_async()), ())
        else:
            uasyncio.run(self.run_async())
