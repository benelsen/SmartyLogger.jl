import esp
esp.osdebug(None)

import ubinascii, machine

CLIENT_ID = ubinascii.hexlify(machine.unique_id())
HOSTNAME = CLIENT_ID

import network, utime

import config

ap = network.WLAN(network.AP_IF)
ap.active(config.wifi_ap)
ap.config(essid=HOSTNAME, config.wifi_ap_pass, authmode=network.AUTH_WPA2_PSK)

wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.config(dhcp_hostname=HOSTNAME)

if not (wlan.active() and wlan.isconnected() and wlan.status() == network.STAT_GOT_IP):
    print('connecting to network...')
    wlan.connect(config.wifi_sta_ssid, config.wifi_sta_pass)

    start = utime.ticks_ms()
    while not wlan.isconnected():
        delta = utime.ticks_diff(utime.ticks_ms(), start)
        if delta > 20000:
            ap.active(True)
            break

    if wlan.isconnected():
        ap.active(False)
        print('network config:', ap.ifconfig())

    print('network config:', wlan.ifconfig())

else:
    ap.active(False)

import gc
import webrepl
webrepl.start()
gc.collect()
