"""
Name: Electrostat.py
Author: Dakota Carter
License: MIT
Description: stop phones from charging all the way up when plugged in. Useful for running a real device selenium grid
and preserving battery health.
"""
import usb.util
import time
import sys

SET_FEATURE = 0x03
CLEAR_FEATURE = 0x01
USB_PORT_FEAT_POWER = (1<<3)
LIBUSB_DEBUG=4

devices = usb.core.find(True)
for index, device in enumerate(devices):
    if "SAMSUNG" in str(device.manufacturer).upper():
        # device.ctrl_transfer(usb.util.CTRL_TYPE_CLASS | usb.util.CTRL_RECIPIENT_OTHER,
        #                      SET_FEATURE, 0, usb.control.FUNCTION_SUSPEND, USB_PORT_FEAT_POWER, 10)


        # test = usb.control.set_feature(device, usb.control.ENDPOINT_HALT)
        # test2 = usb.control.clear_feature(device, 1)
        # print("found phone")
        # print("Phone max power set to", str(power_config.bMaxPower))
        # power_config.bMaxPower = 100
        # print("Setting power to 0")
        # device.set_configuration(power_config)
        # print("Setting config back to device, manually confirm")
        # print("")

        # By default, the kernel will claim the device and make it available via
        # /dev/usb/hiddevN and /dev/hidrawN which also prevents us
        # from communicating otherwise. This removes these kernel devices.
        # Yes, it is weird to specify an interface before we get to a configuration.
        if device.is_kernel_driver_active(0):
            print("Detaching kernel driver")
            device.detach_kernel_driver(0)
            test2 = usb.control.clear_feature(device, 0)
        # device.ctrl_transfer(usb.util.CTRL_TYPE_CLASS | usb.util.CTRL_RECIPIENT_OTHER,
        #                      SET_FEATURE, index, 0, USB_PORT_FEAT_POWER, 10)

        # 2. Configuration
        # A Device may have multiple Configurations, and only one can be active at
        # a time. Most devices have only one. Supporting multiple Configurations
        # is reportedly useful for offering more/less features when more/less
        # power is available.
        ## Because multiple configs are rare, the library allows to omit this:
        ## device.set_configuration(CONFIGURATION_EV3)
        configuration = device.get_active_configuration()

        # 3. Interface
        # A physical Device may have multiple Interfaces active at a time.
        # A typical example is a scanner-printer combo.
        #
        # 4. Alternate setting
        # I don't quite understand this, but they say that if you need Isochronous
        # Endpoints (read: audio or video), you must go to a non-primary
        # Alternate Setting.
        interface = configuration[(0, 0)]

        # 5. Endpoint
        # The Endpoint 0 is reserved for control functions
        # so we use Endpoint 1 here.
        # If an Interface uses multiple Endpoints, they will differ
        # in transfer modes:
        # - Interrupt transfers (keyboard): data arrives soon, with error detection
        # - Isochronous transfers (camera): data arrives on time, or gets lost
        # - Bulk transfers (printer): all data arrives, sooner or later

        endpoint = interface[1]
        usb.util.claim_interface(device, interface)
        bmRequestType, wIndex = usb.control._parse_recipient(interface, usb.util.CTRL_OUT)
        usb.core.Device.ctrl_transfer(device, bmRequestType=usb.util.CTRL_TYPE_CLASS | usb.util.CTRL_RECIPIENT_OTHER,
                                      bRequest=SET_FEATURE, wValue=0, wIndex=usb.control.FUNCTION_SUSPEND,
                                      data_or_wLength=USB_PORT_FEAT_POWER,
                                      timeout=100)

        time.sleep(1)
        usb.util.dispose_resources(device)
        device.reset()
        status = usb.control.get_status(device)

        # Finally!
        print(1)
        endpoint.write(bytes('SUSPEND'))