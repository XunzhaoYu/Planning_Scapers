import subprocess, sys, re

command_argv = sys.argv[1]
#if sys.argv[2]:
#    argv = sys.argv[2]

def execute_commands(commands, print_bool=True):
    for command in commands:
        if print_bool:
            print("\n>>>>>" + " ".join(command))
        subprocess.run(command)

def init():
    commands = []
    commands.append(['sudo', 'yum', 'update'])
    commands.append(['mkdir', 'Lists'])
    commands.append(['mkdir', 'ScrapedApplications'])

    commands.append(['rm', 'UKPlanning/settings_linux.py'])
    commands.append(['rm', 'UKPlanning/pipelines_(UKPlanIt_API).py'])
    commands.append(['rm', 'UKPlanning/spiders/UKPlanIt_API_yu.py'])
    commands.append(['rm', 'UKPlanning/data_to_scrape.numbers'])
    commands.append(['rm', 'UKPlanning/scraper_name.numbers'])

    commands.append(['sudo', 'yum', 'install', 'python-virtualenv'])
    commands.append(['virtualenv', '-p', 'python3.7', 'scraper_env'])
    #commands.append(['source', 'scraper_env/bin/activate'])
    execute_commands(commands)

def install_chromedriver():
    commands = []
    commands.append(['wget', 'https://storage.googleapis.com/chrome-for-testing-public/122.0.6261.111/linux64/chromedriver-linux64.zip'])
    commands.append(['unzip', 'chromedriver-linux64.zip'])
    execute_commands(commands)

def install_chrome():
    commands = []
    commands.append(['wget', 'https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm'])
    commands.append(['sudo', 'yum', 'localinstall', 'google-chrome-stable_current_x86_64.rpm'])
    commands.append(['yum', 'info', 'google-chrome-stable'])
    execute_commands(commands)

def validate():
    commands = []
    commands.append(['python', '--version'])
    commands.append(['python', '-c', 'import sys; print(sys.path)'])
    #commands.append(['chromedriver', '--version'])
    commands.append(['google-chrome', '--version'])
    commands.append(['which', 'google-chrome'])
    execute_commands(commands)

def configure_env():
    commands = []
    commands.append(['pip', 'install', '--upgrade', 'pip'])
    commands.append(['pip', 'install', 'scrapy'])
    commands.append(['pip', 'install', 'scrapy_selenium'])
    commands.append(['pip', 'install', 'pandas'])
    #commands.append(['pip', 'install', 'webdriver-manager'])
    commands.append(['pip', 'install', 'urllib3 <=1.26.18'])
    commands.append(['pip', 'install', 'Twisted==22.10.0'])
    execute_commands(commands)

if command_argv == 'init':
    init()
elif command_argv == 'install_chromedriver':
    install_chromedriver()
elif command_argv == 'install_chrome':
    install_chrome()
elif command_argv == 'validate':
    validate()
elif command_argv == 'configure_env':
    configure_env()
else:
    print('')

# python3 EC2_commands.py init  # subprocess only exists in Python 3.5 and newer. The default Python on Linux is 2.7.18
# source scraper_env/bin/activate
# python EC2_commands.py install_chromedriver
# python EC2_commands.py install_chrome
# (optional) python EC2_commands.py validate
# python EC2_commands.py configure_env