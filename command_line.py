# Run configuration script
import settings
import os
import src.extract_01 as ex


def execute_from_command_line(args):
    # print("Base directory is: ", settings.BASE_DIR)
    print("Arguments passed: %s" % args)
    if len(args) == 1:
        print(len(args))
        print("No argument passed.")
        return False

    if args[1] == "extract":
        stop_time = 300000
        if len(args) > 2:
            try:
                cycle_time_sec = int(args[2])
                print("Launch extraction with cycle time of %d seconds" %
                      cycle_time_sec)
                ex.operate_timer(cycle_time_sec=cycle_time_sec,
                                 stop_time_sec=stop_time)
            except:
                print("Time in seconds must be an integer")
        else:
            print("Launch extraction with cycle time of 1200 seconds")
            cycle_time_sec = 1200
            ex.operate_timer(cycle_time_sec=cycle_time_sec,
                             stop_time_sec=stop_time)
