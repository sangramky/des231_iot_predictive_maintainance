#!/usr/bin/env python3

import json
import time
import threading
import argparse
from kafka import KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import TopicPartition

KAFKA_TOPIC_IN = 'topic_sensor_input'
KAFKA_TOPIC_SYS_PARTITION = 0
KAFKA_BROKERS_IN = '127.0.0.189:9092, 127.0.0.38.9092, 127.0.0.199:9092'
THREAD_LOCK = threading.Lock()

class PartitionConsumerThread(threading.Thread):
	def __init__(self, tp):
		super().__init__()
		self.consumer = None
		self.tp = tp
		self.running = True
		self.daemon = True

	def run(self):
		print(f"Started consumer thread for partition {self.tp.partition}")
		self.consumer = KafkaConsumer(
			#KAFKA_TOPIC_IN, --- let's assign TopicPartition
			bootstrap_servers=KAFKA_BROKERS_IN,
			#auto_offset_reset='latest',
			enable_auto_commit=True,
            consumer_timeout_ms=1000,
			#group_id=None,
			#group_id='motor_consumer_group-MAIN',
			max_poll_records=1,
			value_deserializer=lambda x: x
		)
		self.consumer.assign([self.tp])

		while self.running:
			try:
				# Poll for messages with timeout
				msg_pack = self.consumer.poll(timeout_ms=1000, max_records=1)
				for tp, messages in msg_pack.items():
					if tp != self.tp:
						print(f"Error TP mismatch {tp} in thread for TP {self.tp}")

					for msg in messages:
						data = json.loads(msg.value.decode('utf-8'))
						print("TH-%-3d: [%d] %s:%2d \tMID=%-4s seq_id=%s"
	    						% (self.tp.partition, msg.timestamp, str(msg.key, 'utf-8'),
	      						msg.partition, data['MID'], data['seq_id']))


			except Exception as e:
				if self.running:
					print(f"Error in partition {self.tp.partition}: {e}")

		print(f"Stopped consumer thread for partition {self.tp.partition}")

	def stop(self):
		self.running = False

class RebalanceListener(ConsumerRebalanceListener):
	def __init__(self):
		self.partition_threads = {}
		self.consumer = None

	def on_partitions_revoked(self, revoked):
		with THREAD_LOCK:
			print(f"TPs revoked: {revoked}")
			for tp in revoked:
				if tp in self.partition_threads:
					print(f"Stopping thread for partition {tp.partition}")
					self.partition_threads[tp].stop()
					self.partition_threads[tp].join(timeout=2)
					del self.partition_threads[tp]

	def on_partitions_assigned(self, assigned):
		with THREAD_LOCK:
			print(f"TPs assigned: {assigned}")
			for tp in assigned:
				# Skip if tp.partition == 0 (reserved for Main thread)
				# if tp.partition == KAFKA_TOPIC_SYS_PARTITION:
				# 	print(f"Skip thread creation reserved partition {tp.partition}")
				# 	continue

				if tp not in self.partition_threads:
					print(f"Starting thread for partition {tp.partition}")
					thread = PartitionConsumerThread(tp)
					self.partition_threads[tp] = thread
					thread.start()

def main():
	topic = KAFKA_TOPIC_IN
	#topic = 'dummy_topic_not_exists'
	consumer = KafkaConsumer(
		#topic,
		bootstrap_servers=KAFKA_BROKERS_IN,
		group_id='motor_consumer_group-MAIN',
		auto_offset_reset='latest',
		enable_auto_commit=True,
		max_poll_records=500, #default is 500
		#value_deserializer=lambda x: x
        #consumer_timeout_ms=2000,
		metadata_max_age_ms=1000,
		#fetch_min_bytes=1048576,
		#fetch_max_wait_ms=20000,
	)

	# Subscribe with rebalance listener
	rebalance_listener = RebalanceListener()
	consumer.subscribe([topic], listener=rebalance_listener)
	#consumer.assign([TopicPartition(topic, KAFKA_TOPIC_SYS_PARTITION)])

	print(f"Subscribed to topic: {topic}")
	print("Waiting for partition assignments...")

	try:
		# Keep the main thread alive
		while True:
			#consumer.topics()
			#partitions = consumer.partitions_for_topic(topic)
			#if partitions:
			#	print(f"Current partitions for topic '{topic}': {partitions}")

			msgs = consumer.poll(timeout_ms=5000)

			# print(f"Main thread polled {len(msgs)} messages")
			time.sleep(1)

	except KeyboardInterrupt:
		print("\nShutting down...")

	finally:
		# Stop all partition threads
		for thread in rebalance_listener.partition_threads.values():
			thread.stop()
		for thread in rebalance_listener.partition_threads.values():
			thread.join(timeout=2)
		consumer.close()

if __name__ == "__main__":
	main()
