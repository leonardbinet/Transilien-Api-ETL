# Run configuration script
import settings
import os


def execute_from_command_line(*args):
    print("Arguments passed: %s" % args)
    if len(args) == 1:
        print("No argument passed.")
    print("Base directory is: ", settings.BASE_DIR)
