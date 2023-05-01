FROM python:3.10.10-slim
RUN mkdir webscrapingApp
COPY requirements.txt webscrapingApp/
COPY webscraping.py webscrapingApp/
RUN apt-get update && apt-get install apt-file -y && apt-file update && apt-get install vim -y
RUN pip3 install -r webscrapingApp/requirements.txt

WORKDIR /webscrappinApp
CMD [ "python3", "webscrapping.py" ]

#docker build -t <hubname>:<reponmae_image name>:version -f <docker file path> .