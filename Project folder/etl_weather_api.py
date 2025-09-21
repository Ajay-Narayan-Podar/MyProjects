import requests
url="https://api.open-meteo.com/v1/forecast?latitude=27.7172&longitude=85.3240&current_weather=true"
response=requests.get(url)

if response.status_code==200:
    data=response.json() #gives a python dictionary
    weather=data['current_weather']
    print(f"DateTime: {weather['time']}")
    print(f"Temperature: {weather["temperature"]}°C") #special character was bought using charmap.
    print(f"windspeed: {weather['windspeed']} km/hr")
else:
    print(f'Error: {response.status_code}')