import subprocess, sys, re

IPv4 = sys.argv[1]
command_argv = sys.argv[2]
key_pairs = '/Users/xunzhaoyu/Documents/Work/UKPlanning.pem'
local_path = '/Users/xunzhaoyu/Documents/Work/24-01 to 24-06 Warwick/Planning&Nimbyism/'
EC2_path = f'ec2-user@ec{IPv4}.eu-west-2.compute.amazonaws.com:'
local_lib_path = '/Users/xunzhaoyu/Documents/Anaconda/anaconda3/lib/python3.7/site-packages/'
EC2_lib_path = EC2_path + 'scraper_env/lib/python3.7/site-packages/'

def execute_commands(commands, print_bool=True):
    for command in commands:
        if print_bool:
            print("\n>>>>>" + " ".join(command))
        subprocess.run(command)

def init():
    commands = []
    # upload UKPlanning:  # Main Project
    commands.append(['scp', '-i', key_pairs, '-r', local_path + 'UKPlanning', EC2_path + 'UKPlanning'])
    # upload scrapy.cfg:
    commands.append(['scp', '-i', key_pairs, local_path + 'scrapy.cfg', EC2_path + 'scrapy.cfg'])
    # upload EC_commands:
    commands.append(['scp', '-i', key_pairs, local_path + 'EC2_commands.py', EC2_path + 'EC2_commands.py'])
    execute_commands(commands)
    upload_settings()

def upload_data(auths=['Aberdeen', 'Bassetlaw']):
    # need mkdir Data on EC2 instances
    commands = []
    for auth in auths:
        commands.append(['scp', '-i', key_pairs, '-r', local_path+f'Data/{auth}', EC2_path+f'Data/{auth}'])
    execute_commands(commands)

def upload_settings():
    #command = ['scp', '-i', key_pairs, local_path + 'UKPlanning/settings.py', EC2_path + 'UKPlanning/settings.py']
    command = ['scp', '-i', key_pairs, local_path + 'UKPlanning/settings_linux.py', EC2_path + 'UKPlanning/settings.py']
    execute_commands([command])

def upload_middlewares():
    #command = ['scp', '-i', key_pairs, local_lib_path + 'scrapy_selenium/middlewares.py', EC2_lib_path + 'scrapy_selenium/middlewares.py']
    command = ['scp', '-i', key_pairs, local_lib_path + 'scrapy_selenium/middlewares_linux.py', EC2_lib_path + 'scrapy_selenium/middlewares.py']
    execute_commands([command])

def upload_scraper():
    command = ['scp', '-i', key_pairs, local_path + 'UKPlanning/spiders/UKPlanning_Scraper.py', EC2_path + 'UKPlanning/spiders/UKPlanning_Scraper.py']
    execute_commands([command])

def upload_EC2_commands():
    command = (['scp', '-i', key_pairs, local_path + 'EC2_commands.py', EC2_path + 'EC2_commands.py'])
    execute_commands([command])


if command_argv == 'init':
    init()
elif command_argv == 'Data':
    upload_data()
elif command_argv == 'settings':
    upload_settings()
elif command_argv == 'middlewares':
    upload_middlewares()
elif command_argv == 'scraper':
    upload_scraper()
elif command_argv == 'EC2_commands':
    upload_EC2_commands()
else:
    print('')

# python /Users/xunzhaoyu/Documents/Work/24-01\ to\ 24-06\ Warwick/Planning\&Nimbyism/local_commands.py 2-18-132-203-252 init
# python /Users/xunzhaoyu/Documents/Work/24-01\ to\ 24-06\ Warwick/Planning\&Nimbyism/local_commands.py 2-18-132-203-252 Data
# python /Users/xunzhaoyu/Documents/Work/24-01\ to\ 24-06\ Warwick/Planning\&Nimbyism/local_commands.py 2-18-132-203-252 middlewares