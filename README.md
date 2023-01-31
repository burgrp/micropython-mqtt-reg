# Registers over MQTT for MicroPython

Check register protocol: https://github.com/burgrp/mqtt-reg.

Example:
```python
class TestHandler:
    registers = {
        'a': 1,
        'b': 2,
        'c': 3
    }

    def get_names(self):
        return self.registers.keys()

    def get_meta(self, name):
        return {
            'device': 'test',
            'title': 'test register ' + name
        }

    def get_value(self, name):
        return self.registers[name]

    def set_value(self, name, value):
        self.registers[name] = value


registry = mqtt_reg.Registry(
    TestHandler(),
    wifi_ssid=site_config.wifi_ssid,
    wifi_password=site_config.wifi_password,
    mqtt_broker=site_config.mqtt_broker,
    ledPin=4,
    debug=site_config.debug
)

registry.start()

```