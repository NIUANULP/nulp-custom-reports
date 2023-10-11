
## Installation (Progress Report API)

  Clone the latest branch repo from below and change dir to project directory. Add .env manually.
```bash
  https://github.com/NIUANULP/nulp-custom-reports.git
  
```
After repo is cloned, Create a virtual env to install libraries.
```bash
python3 -m venv ~envs/sunbird-reports/
source ~envs/sunbird-reports/bin/activate
```
This will activate the venv.

## Installing libraries

Once the venv is activated , Change to the directory with requirements.txt and install the libraries.

```bash
pip install -r requirements.txt
```

## Starting the FastAPI server.

Once all requirements are installed, The fastapi can be started with following commands.

```bash
nohup uvicorn main:app --reload --host [hostip] --port [port] &
```
Replace the host with 0.0.0.0
Replace the port with the specific opened port.

## Build FastAPI docker image for this service.
Clone the repo and user the docker file to create the docker image
```bash
sudo docker build -t sunbird-reports:latest .
```
## Run Docker image in - detached mode.
sudo docker run -d -p PORT:PORT IMAGEID

## Change Variables from env
Replace or add env manually and replace variables.







