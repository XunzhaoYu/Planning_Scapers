Planning_Scapers


----- ----- ----- Project structure ----- ----- -----   
The project is structured as follows:  
.  
├── 📂 Lists/: The list of all applications from Local Authorities.   
├── 📂 ScrapedApplications/: The data and documents scraped from the Local Authorities/PlanIt API.  
├── 📂 UKPlanning/: All scripts/scrapers.  
│ &nbsp; &nbsp; &nbsp; ├── requirements.txt (to be added.)    
│ &nbsp; &nbsp; &nbsp; ├── 📂 general/:                                      # General-purpose scraper logic (not tied to a specific framework)  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 base_scraper.py         # Common Scrapy Spider base class  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 utils.py                # General utility functions  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 items.py                #  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── parsers.py                 # Shared parsing utilities  
│ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; ├── core/                             # Core modules shared by all frameworks and scrapers  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── http_utils.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── proxy_manager.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── recaptcha_solver.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── log_utils.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── settings_base.py              # Base settings file (inherited by each framework)  
│ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; ├── frameworks/                       # Framework-specific templates and logic  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── idox/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── items.py                  # Item definitions for Idox  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── base_spider.py            # Idox base spider class  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── pipelines.py              # Default pipeline for Idox  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── middlewares.py            # Default middleware for Idox  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── settings.py               # Default Idox framework settings  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── atrium/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── items.py                  # Item definitions for Atrium  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── base_spider.py            # Atrium base spider class  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── pipelines.py              # Default pipeline for Atrium  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── middlewares.py            # Default middleware for Atrium  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── settings.py               # Default Atrium framework settings  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── ...                           # Future frameworks can be added here  
│ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; ├── 📂 scrapers/:                         # Individual scraper instances for each framework  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Idox/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Idox1_scraper.py       # Idox scraper 1 (inherits from IdoxBaseSpider)  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Idox2_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── ...  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Atrium/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Atrium1_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Atrium2_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── ...  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │    
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── PlanningExplorer/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── PlanningExplorer1_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── ...  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── CCED_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Custom_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 Tascomi.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── Thames.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │   
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── others/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── pdf_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── sitemap_scraper.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; └── ...   
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp;  
│ &nbsp; &nbsp; &nbsp; ├── 📂 middlewares/:                      # Globally available middleware modules  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── rotating_proxy_mw.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── recaptcha_mw.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── retry_mw.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── user_agent_mw.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── custom/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── idox_proxy_mw.py          # Idox-specific custom middleware  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; └── atrium_auth_mw.py         # Atrium-specific custom middleware  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp;  
│ &nbsp; &nbsp; &nbsp; ├── 📂 pipelines/:                        # Globally available pipeline modules   
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── json_export_pipeline.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── db_pipeline.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── s3_pipeline.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── custom/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── idox_custom_pipeline.py   # Idox-specific custom pipeline  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; └── atrium_custom_pipeline.py # Atrium-specific custom pipeline  
│ &nbsp; &nbsp; &nbsp; │  
│ &nbsp; &nbsp; &nbsp; ├── 📂 tools/:                          # External tool modules  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── recaptcha/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── solver_2captcha.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── solver_anticaptcha.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── solver_manual.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── ip_rotation/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── __init__.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── rotator_proxy_service.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── rotator_custom_pool.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── notifier.py                   # Notification tools (Slack, email, etc.)   
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp;  
│ &nbsp; &nbsp; &nbsp; ├── 📂 configs/:                          # Project-wide configuration files  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── 📄 settings.py            # Global Scrapy settings  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── frameworks/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; ├── idox_settings.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── atrium_settings.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; └── scrapers/  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── council_a_settings.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── council_b_settings.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; └── atrium_site_1_settings.py  
│ &nbsp; &nbsp; &nbsp; │ &nbsp; &nbsp; &nbsp;  
│ &nbsp; &nbsp; &nbsp; └── tests/:                            # Unit and integration tests  
│ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── test_core.py  
│ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── test_frameworks.py  
│ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ├── test_scrapers.py  
│ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; └── test_middlewares.py  
├── 📄 EC2_commands: EC2 shell script for configuring EC2 instances.   
├── 📄 local_commands: Local shell script for configuring EC2 instances.  
├── 📄 scraper_document.pdf: User guidance for using scrapers on local machines (Scrapers).  
├── 📄 scrapy.cfg: Scrapy entry configuration.  
└── 📄 README.md

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






