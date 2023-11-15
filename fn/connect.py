# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 10:05:03 2023

@author: v0042447
"""
import os, environ
import psycopg2

env = environ.Env()

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Take environment variables from .env file
environ.Env.read_env(os.path.join(BASE_DIR, 'env_file'))

def conn():
    
    # Establish a connection
    conn = psycopg2.connect(
        dbname=os.getenv('REDSHIFT_NAME'),
        user=os.getenv('REDSHIFT_USERNAME'),
        password=os.getenv('REDSHIFT_PASS'),
        host=os.getenv('REDSHIFT_HOST'),
        port=os.getenv('REDSHIFT_PORT')
    )
    
    return conn