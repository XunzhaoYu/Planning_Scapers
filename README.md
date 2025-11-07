Planning_Scapers


----- ----- ----- Project structure ----- ----- ----- 

Folder 'List': The list of all applications from Local Authorities.

Folder 'ScrapedApplications': The data and documents scraped from the Local Authorities/PlanIt API.

Folder 'UKPlanning': All scripts/scrapers.

scraper_document.pdf: User guidance for using scrapers on local machines (Scrapers).

local_commands: Local shell script for configuring EC2 instances.

EC2_commands: EC2 shell script for configuring EC2 instances. 

UKPlanning/  
│  
├── scrapy.cfg                        # Scrapy entry configuration  
├── README.md  
├── requirements.txt  
│  
├── general/                          # General-purpose scraper logic (not tied to a specific framework)  
│   ├── __init__.py  
│   ├── base_scraper.py               # Common Scrapy Spider base class  
│   ├── utils.py                      # General utility functions  
│   └── parsers.py                    # Shared parsing utilities  
│  
├── core/                             # Core modules shared by all frameworks and scrapers  
│   ├── __init__.py  
│   ├── http_utils.py  
│   ├── proxy_manager.py  
│   ├── recaptcha_solver.py  
│   ├── log_utils.py  
│   └── settings_base.py              # Base settings file (inherited by each framework)  
│  
├── helpers/                          # External helper modules  
│   ├── __init__.py  
│   ├── recaptcha/  
│   │   ├── __init__.py  
│   │   ├── solver_2captcha.py  
│   │   ├── solver_anticaptcha.py  
│   │   └── solver_manual.py  
│   ├── ip_rotation/  
│   │   ├── __init__.py  
│   │   ├── rotator_proxy_service.py  
│   │   └── rotator_custom_pool.py  
│   └── notifier.py                   # Notification tools (Slack, email, etc.)  
│  
├── frameworks/                       # Framework-specific templates and logic  
│   ├── __init__.py  
│   │  
│   ├── idox/  
│   │   ├── __init__.py  
│   │   ├── items.py                  # Item definitions for Idox  
│   │   ├── base_spider.py            # Idox base spider class  
│   │   ├── pipelines.py              # Default pipeline for Idox  
│   │   ├── middlewares.py            # Default middleware for Idox  
│   │   └── settings.py               # Default Idox framework settings  
│   │  
│   ├── atrium/  
│   │   ├── __init__.py  
│   │   ├── items.py                  # Item definitions for Atrium  
│   │   ├── base_spider.py            # Atrium base spider class  
│   │   ├── pipelines.py              # Default pipeline for Atrium  
│   │   ├── middlewares.py            # Default middleware for Atrium  
│   │   └── settings.py               # Default Atrium framework settings  
│   │  
│   └── ...                           # Future frameworks can be added here  
│  
├── scrapers/                         # Individual scraper instances for each framework  
│   ├── __init__.py  
│   │  
│   ├── idox/  
│   │   ├── __init__.py  
│   │   ├── council_a_spider.py       # Example spider (inherits from IdoxBaseSpider)  
│   │   ├── council_b_spider.py  
│   │   ├── council_c_spider.py  
│   │   └── ...  
│   │  
│   ├── atrium/  
│   │   ├── __init__.py  
│   │   ├── atrium_site_1_spider.py  
│   │   ├── atrium_site_2_spider.py  
│   │   └── ...  
│   │
│   └── others/  
│       ├── __init__.py  
│       ├── pdf_scraper.py  
│       ├── sitemap_scraper.py  
│       └── ...   
│
├── middlewares/                      # Globally available middleware modules  
│   ├── __init__.py  
│   ├── rotating_proxy_mw.py  
│   ├── recaptcha_mw.py  
│   ├── retry_mw.py  
│   ├── user_agent_mw.py  
│   └── custom/  
│       ├── idox_proxy_mw.py          # Idox-specific custom middleware  
│       └── atrium_auth_mw.py         # Atrium-specific custom middleware  
│  
├── pipelines/                        # Globally available pipeline modules   
│   ├── __init__.py  
│   ├── json_export_pipeline.py  
│   ├── db_pipeline.py  
│   ├── s3_pipeline.py  
│   └── custom/  
│       ├── idox_custom_pipeline.py   # Idox-specific custom pipeline  
│       └── atrium_custom_pipeline.py # Atrium-specific custom pipeline  
│  
├── configs/                          # Project-wide configuration files  
│   ├── settings_global.py            # Global Scrapy settings  
│   ├── frameworks/  
│   │   ├── idox_settings.py  
│   │   └── atrium_settings.py  
│   └── scrapers/  
│       ├── council_a_settings.py  
│       ├── council_b_settings.py  
│       └── atrium_site_1_settings.py  
│  
└── tests/                            # Unit and integration tests  
    ├── test_core.py  
    ├── test_frameworks.py  
    ├── test_scrapers.py  
    └── test_middlewares.py  

----- ----- ----- Run scraper on local machines ----- ----- -----

See scraper_document.pdf for details.
 

----- ----- ----- Configure EC2 instances ----- ----- ----- 
1. Follow the instructions to start a new EC2 instance: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html

2. Get your EC2 instance's public IPv4 DNS, i.e: ec2-18-130-206-213
   <img src="https://github.com/XunzhaoYu/Planning_Scapers/blob/main/img/connection-prereqs-console2.png" width="75%">

4. Execute shell command from your local machine:
```
	python {your local command path}/local_commands.py {your EC2 instance's IPv4 DNS} init
```

5. Execute shell commands from your EC2 instance:
```
	python3 EC2_commands.py init
	source scraper_env/bin/activate
```

6. Execute shell command from your local machine:
```
	python {your local command path}/local_commands.py {your EC2 instance's IPv4 DNS} Data
```

7. Execute shell commands from your EC2 instance:
```
	python EC2_commands.py install_chromedriver
	python EC2_commands.py install_chrome
	python EC2_commands.py configure_env
	cd UKPlanning
	python main.py
```

----- ----- ----- Develop new scraper ----- ----- -----      
Currently, the scraper (UKPlanning_Scraper) is able to scrape most information items from Idox portals.         
To develop new scrapers by adapting the existing scraper, you can create a new scraper class as a subclass of UKPlanning_Scraper and overwrite its parse methods.     
           
           
--- --- END of UKPlanning_Scraper Guidance --- ---









           
```
###########        
Below is the guidance for UKPlanIt_API.py, not for local authorities.      Please ignore them.      
###########
```   
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






