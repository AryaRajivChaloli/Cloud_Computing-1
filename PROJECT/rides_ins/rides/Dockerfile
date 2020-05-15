FROM python
ADD . /home/ubuntu/CC_instance/rides_ins/rides
WORKDIR /home/ubuntu/CC_instance/rides_ins/rides
ENV TEAM_NAME="CC_0032_0210_0253_0646"
RUN pip3 install -r requirements.txt
RUN echo $TEAM_NAME
CMD [ "python3", "/home/ubuntu/CC_instance/rides_ins/rides/rides.py"]
EXPOSE 80


