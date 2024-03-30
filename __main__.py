from adisconfig import adisconfig
from log import Log
from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from json import loads, dumps
from pprint import pprint
from pathlib import Path
from uuid import uuid4
from os import mkdir
from pymongo import MongoClient
from yuki import prompt, db
from openai import OpenAI
from tqdm import tqdm

import requests

class Speech_Generator:
    name="yuki-speech_generator"

    def __init__(self):
        self._config=adisconfig('/opt/adistools/configs/yuki-speech_generator.yaml')

        self._log=Log(
            parent=self,
            rabbitmq_host=self._config.rabbitmq.host,
            rabbitmq_port=self._config.rabbitmq.port,
            rabbitmq_user=self._config.rabbitmq.user,
            rabbitmq_passwd=self._config.rabbitmq.password,
            debug=self._config.log.debug,
            )

        self._prompt=prompt.Prompt()
        self._openai=OpenAI(api_key=self._config.openai.api_key)

        self._rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                heartbeat=0,
                host=self._config.rabbitmq.host,
                port=self._config.rabbitmq.port,
                credentials=PlainCredentials(
                    self._config.rabbitmq.user,
                    self._config.rabbitmq.password
                )
            )
        )

        self._rabbitmq_channel = self._rabbitmq_conn.channel()
        self._rabbitmq_channel.basic_consume(
            queue='yuki-speech_requests',
            auto_ack=True,
            on_message_callback=self._speech_request
        )

        self._media_dir=Path(self._config.directories.media)
        self._renders_dir=Path(self._config.directories.renders)

        self._db=db.DB(self)


    def _notify_superivser(self, data):
        self._rabbitmq_channel.basic_publish(
                exchange="",
                routing_key="yuki-generation_finished",
                body=data
            )

    def _speech_request(self, channel, method, properties, body):
        video_uuid=body.decode('utf8')
        video=self._db.get_video(video_uuid)

        for scene in tqdm(video['script']):
            print(scene['speech'])
            response = self._openai.audio.speech.create(
                model=self._config.openai.model,
                voice=self._config.openai.voice,
                input=scene['speech'],
            )

            file=self._media_dir.joinpath(video['video_uuid']).joinpath(f"scene_{video['script'].index(scene)+1}.aac")
            response.write_to_file(file)

            video['script'][video['script'].index(scene)]['audio']=str(file)

        self._db.update_video(video_uuid, video, 'audio')
        self._notify_superivser(video_uuid)

    def start(self):
        self._rabbitmq_channel.start_consuming()

    def stop(self):
        self._rabbitmq_channel.stop_consuming()

if __name__=="__main__":
    speech_generator=Speech_Generator()
    speech_generator.start()
