# med_scrapper_mx

> 🚧 work in progress 

This project is a data scraping and analysis pipeline focused on medical directories in Mexico. It collects, processes, and analyzes information about: 
* Doctors
* Their specialties
* Locations
* Services offering
* Appointment prices
* If there are remote appointments

The workspace includes Python scripts for web scraping, data transformation, and exploratory data analysis (EDA) using Jupyter notebooks. Visualizations and summary statistics are generated to provide insights into geographic distribution, pricing, and service offerings. The project is organized for reproducibility and further research in healthcare data analytics.

To run the current scrapping pipeline use:
```
python3 main.py
```

This command line will scrapp main url, using 3 different strategies:
* scrapping by city
* scrapping by state
* scrapping by city-state concatenated

🚀 Updates are comming...