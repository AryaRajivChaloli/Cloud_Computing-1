FROM python
ADD . /home/ubuntu/Assignment2/rides
WORKDIR /home/ubuntu/Assignment2/rides
ENV TEAM_NAME="CC_0032_0210_0253_0646"
RUN pip3 install -r requirements.txt
RUN echo $TEAM_NAME
CMD [ "python3", "/home/ubuntu/Assignment2/rides/rides.py"]
EXPOSE 8000


