import pytz
from dateutil import parser
import os
import shutil
import ipywidgets as widgets
from rapidfuzz import process

def check_status_error(res,place=""):
    if res.status_code!=200:
        raise Exception(f'Couldn\'t get {place} data. Please check url or auth token\n',res.content)
    
def cleaned_string(string):
    new_str = ""
    for char in string:
        if char in "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ":
            new_str+=char
    return new_str

def create_dropdown(options,value,description):
    dropdown = widgets.Dropdown(
        value=value,
        options=options,
        description=description,
        disabled=False,
        style={'description_width': 'initial'}
    )
    return dropdown

def convert_timezone(dt_str, to_tz_str):
    dt = parser.parse(dt_str) 
    to_tz = pytz.timezone(to_tz_str)
    dt = dt.astimezone(to_tz)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def change_reservation_timezone(x):
    tz = "America/Chicago"
    x['starts'] = convert_timezone(str(x['starts']), tz)
    x['ends'] = convert_timezone(str(x['ends']), tz)
    x['date purchased'] = convert_timezone(str(x['date purchased']), tz)
    return x

def change_blackout_timezone(x):
    tz = "America/Chicago"
    x['starts'] = convert_timezone(str(x['starts']), tz)
    x['ends'] = convert_timezone(str(x['ends']), tz)
    return x

def remake_temp_dir():
    temp_folder_path = 'temp'
    if os.path.exists(temp_folder_path):
        shutil.rmtree(temp_folder_path)
    os.makedirs(temp_folder_path)

def make_model_output_dir():
    dir_path = 'model_outputs'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def searchString(string,options):
    choices = options
    def get_descriptions(choices):
        return [item[0] for item in choices]

    descriptions = get_descriptions(choices)
    query = string

    matches = process.extract(query, descriptions, limit=20)

    searched_options = [next(item for item in choices if item[0] == description) for description, score,idx in matches]
    return searched_options
