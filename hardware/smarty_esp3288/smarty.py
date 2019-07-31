import utime
from machine import Pin, UART
from umqtt.simple import MQTTClient
import uselect
import uos

def run(HOSTNAME, CLIENT_ID, BROKER_HOST):

    print('Disabling REPL on UART')
    uos.dupterm(None, 1)

    en1 = Pin(16, Pin.OUT, value=1)
    rx = UART(0, 115200, rxbuf = 1024)

    def on_msg(topic, msg):
        if topic == b"smarty_control":
            if msg == b"reset":
                machine.reset()

    c = MQTTClient(HOSTNAME, BROKER_HOST)
    c.set_callback(on_msg)
    c.connect()
    c.publish(b"smarty_control", b"logon" + b"_" + HOSTNAME + b"_" + CLIENT_ID)
    c.subscribe(b"smarty_control")

    poll = uselect.poll()
    poll.register(rx, uselect.POLLIN)

    print('enabling')
    en1.value(0)

    while True:
        c.check_msg()
        evs = poll.poll(10000)

        for ev in evs:
            if ev[0] == rx:
                if ev[1] == uselect.POLLERR:
                    print('error')
                elif ev[1] == uselect.POLLIN:
                    print('data ready')
                    data = b""
                    while rx.any() > 0:
                        data = data + rx.read()
                        utime.sleep_ms(5)
                    c.publish(b"smarty_data", data)
                    print(str(len(data)) + ' bytes sent')


    print('disabling')
    en1.value(1)

    c.publish(b"smarty_control", b"logoff" + b"_" + HOSTNAME + b"_" + CLIENT_ID)
    c.disconnect()
