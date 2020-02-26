import machine, utime, uos
from smarty import run
from machine import Pin, UART
from usocket import getaddrinfo, AF_INET
from sys import print_exception

import config

en1 = Pin(config.pin_enable, Pin.OUT, value=1)

try:
    print('Starting in 10s')
    utime.sleep(10)
    addr = getaddrinfo(config.mqtt_host, config.mqtt_port, AF_INET)[0][-1]
    run(HOSTNAME, CLIENT_ID, addr[0], addr[1], config.mqtt_user, config.mqtt_pass, config.pin_enable, True)

except KeyboardInterrupt:
    print('Enabling REPL on UART')
    en1.value(1)
    uart = machine.UART(0, 115200)
    uos.dupterm(uart, 1)

except Exception as e:
    print('Enabling REPL on UART')
    print_exception(e)
    en1.value(1)
    uart = machine.UART(0, 115200)
    uos.dupterm(uart, 1)

    print('Resetting in 30s')
    utime.sleep(30)
    machine.reset()
