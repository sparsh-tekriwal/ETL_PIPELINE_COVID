# COVID ETL Pipeline 


Instructions to set up the cronjob :

1. Open Terminal
2. Edit the crontab using command crontab -e
3. Schedule the execution of Egen_Solutions.py everyday at 9am by adding the following line of code:
    0 9 * * * /home/ETL_PIPELINE_COVID/Egen_Solutions.py
