#!/usr/bin/env bash

# Print execution date time
echo `date`

# Change directory into where calc_kub_mean_hourly_script.py script is located.
echo "Changing into ~/data_integration/calc_kub_mean_hourly_cron"
cd /home/uwcc-admin/data_integration/calc_kub_mean_hourly_cron
echo "Inside `pwd`"

# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install required libraries and packages using pip.
if [ ! -f "calc_hourly_kub_mean.log" ]
then
    echo "Installing pandas"
    pip install pandas
fi

# Run calc_kub_mean_hourly_script.py script.
echo "Running calc_kub_mean_hourly_script.py. Logs Available in calc_hourly_kub_mean.log file."
#cmd >>file.txt 2>&1
#Bash executes the redirects from left to right as follows:
#  >>file.txt: Open file.txt in append mode and redirect stdout there.
#  2>&1: Redirect stderr to "where stdout is currently going". In this case, that is a file opened in append mode.
#In other words, the &1 reuses the file descriptor which stdout currently uses.
python calc_hourly_kub_mean.log >> calc_hourly_kub_mean.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
