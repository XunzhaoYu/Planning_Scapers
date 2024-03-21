Planning_Scapers


----- ----- ----- Project structure ----- ----- ----- 

Folder 'Data': The processed data.

Folder 'Data_Temp': The raw data scraped from the Local Authorities/PlanIt API.

Folder 'UKPlanning': All scripts.

local_commands: local shell script for configuring EC2 instances.

EC2_commands: EC2 shell script for configuring EC2 instances. 


----- ----- ----- Run scraper on local machine ----- ----- ----- 
1. Put the source csv files in 'Data' folder.

2. Set the local authority to be scraped: (UKPlanning/spiders/UKPlanning_Scraper.py, __init__())
	self.auth = auth_names[{the authority index}]  # auth_names include all authorities loaded from 'Data' folder, change {the authority index} to set self.auth as the authority to scrape.

3. Set settings:
	CLOUD_MODE: if you need upload the scraped data to Cloud storage, set it to 'True'.
	DEVELOPMENT_MODE: if you are developing scraper for new authority portals, set it to 'True' and the scraper will scrape a small set of sampled applications. Otherwise, the scraper will scrape all applications.
	PRINT: if you need to debug, set it to 'True'.

4. If it is the first time to run the scraper, set the init index:  (UKPlanning/spiders/UKPlanning_Scraper.py, __init__()) 
	self.init_index = {the index you want to start}  # if you want to start from the 100th application of a given authority portal, set {the index you want to start} to 99.

5. Run main.py to execute scraper. Data will be collected and stored in 'Data_Temp' folder.
 

----- ----- ----- Configure EC2 instances ----- ----- ----- 
1. Follow the instructions to start a new EC2 instance: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html

2. Get your EC2 instance's public IPv4 DNS, i.e: ec2-18-130-206-213

3. Execute shell command from your local machine: 
	python {your local command path}/local_commands.py {your EC2 instance's IPv4 DNS} init

4. Execute shell commands from your EC2 instance:
	python3 EC2_commands.py init
	source scraper_env/bin/activate

5. Execute shell command from your local machine: 
	python {your local command path}/local_commands.py {your EC2 instance's IPv4 DNS} Data

6. Execute shell commands from your EC2 instance:
	python EC2_commands.py install_chromedriver
	python EC2_commands.py install_chrome
	python EC2_commands.py configure_env
	cd UKPlanning
	python main.py













Below is the guidance for UKPlanIt_API.py, not for local authorities.

----- ----- ----- UKPlanIt APIs ----- ----- ----- 

File 'main.py' contains all APIs related to the scraper. Most APIs contain two parameters which are used to clarify the range of authorities to scrape or process. There are 424 authorities.

    scrape(start, end): To scrape data from the PlanIt API. Results are stored in 'Data_Temp'.
        i.e. scrape(2, 10) will scrape applications from the 2nd to the 10th authorities.
             scrape(5, 5) will scrape applications from the 5th authority.
    append_all(temp): append all csv files into a single csv file. By default, temp = 'True'.
        i.e. append_all(temp=True) will append all csv files in 'Data_Temp' folder.
             append_all(temp=False) will append all csv files in 'Data' folder.
    
    inverse(start, end): The scraped raw data is stored in an inverse order. This method will make applications in csv files stored in a chronological order. 'Data_Temp' -> 'Data_Temp'.
    append_by_year(start, end): append csv files from each authority by years. 'Data_Temp' -> 'Data'.


    
----- ----- ----- Quick start ----- ----- ----- 

Run the following pieces of code to get a csv file with applications from the first 10 authorities.
    
Option1:

	scrape(1, 10)

	append_all()
    
Option2:

	scrape(1, 10)

	inverse(1, 10)

	append_by_year(1, 10)

	append_all(False)

Two options will produce the same csv file named "UKPlanning.csv". But option2 will also produce many csv files in 'Data' folder, these files are useful for further comments and documents scraping. 






