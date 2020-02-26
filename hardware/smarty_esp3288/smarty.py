import utime
from machine import Pin, UART
from umqtt.simple import MQTTClient
import uselect
import uos

def run(hostname, client_id, mqtt_host, mqtt_port, mqtt_user, mqtt_pass, pin_enable, ssl = False):

    print('Disabling REPL on UART')
    uos.dupterm(None, 1)

    en1 = Pin(pin_enable, Pin.OUT, value=1)
    rx = UART(0, 115200, rxbuf = 1024)

    def on_msg(topic, msg):
        print(topic, msg)
        if topic == b"smarty_control":
            if msg == b"reset":
                machine.reset()

    mqtt_client_id = client_id + b"_smarty"

    c = MQTTClient(mqtt_client_id, mqtt_host, mqtt_port, mqtt_user, mqtt_pass,
        keepalive = 60,
        ssl = ssl)
    c.lw_topic = b"smarty_control"
    c.lw_msg = b"logoff/" + mqtt_client_id + b"/lastwill"
    c.set_callback(on_msg)
    c.connect()
    utime.sleep_ms(1000)
    c.publish(b"smarty_control", b"logon/" + mqtt_client_id)
    # c.subscribe(b"smarty_control")

    poll = uselect.poll()
    poll.register(rx, uselect.POLLIN)

    print('Requesting data')
    en1.value(0)

    while True:
        # c.check_msg()
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

    c.publish(b"smarty_control", b"logoff/" + mqtt_client_id)
    c.disconnect()
