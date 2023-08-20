from configparser import  ConfigParser

def config(filename="weather_api_key.ini", section="openweather"):
    parser = ConfigParser()
    parser.read(filename)   
    if parser.has_section(section):
        params = parser.items(section)
        return params[0][1]
    else:
        raise Exception(f'Section {section} not found in {filename}')