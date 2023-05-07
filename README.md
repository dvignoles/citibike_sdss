citibike_sdss
==============================

A spatial decision support system (SDSS) for NYC Citi Bike dock placement & expansion


## Getting started

python3 in a linux environment is required. Using conda is suggested, but venv should work as well. 

See `./Makefile` for details about what each command does. 
   
```
# create python environemtn
make create_environment
conda activate citibike_sdss

# install python developer dependencies
make dev_requirements

# create .env project config file at root
make create_dot_env

# configure aws access
aws configure

# <enter acess key>
# <enter secret key>
# accept defaults

# download from aws s3 to ./data
make sync_data_from_s3

# if desired, upload data to s3
make sync_data_to_s3
```

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets.
    │   ├── prepared       <- Cleaned base datasets, ready for analysis. 
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into aggregated/transformed/composite features
    │   │   └── build_features.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── .pre-commit-config.yaml <- pre-commit file to run code formatting & linting
    └── setup.cfg            <- configuration for isort, black, flake8

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
