#!/bin/bash

# target machines (BRC1 MRC5 KSV8)
target_hosts=("127.0.0.199" "127.0.0.189" "127.0.0.38")
script_dir="/home/siddaraj/iisc/des231/project/"
script_name="motor_consumer.py"
remote_install_dir="/tmp/"
user='root'

# Install the script on all target machines
echo ===== SCP Start =====
for host in "${target_hosts[@]}"; do
	echo "Copying script to $host..."
	scp "$script_dir/$script_name" "$user@$host:$remote_install_dir"
done
echo ===== SCP Done =====

# Monitor and relaunch the script if not running
for (( i = 0; ;i += 1 )); do
	printf "\nIteration-$i\n"
	for host in "${target_hosts[@]}"; do
		# Check if the script is running
		pid=$(ssh "$user@$host" "pgrep -f $script_name")

		if [ -n "$pid" ]; then
			echo "$host: Script is already running (PID: $pid). Skipping restart."
		else
			echo "$host: Script is not running. Launching..."
			#ssh "$user@$host" "nohup $remote_install_dir/$script_name > /dev/null 2>&1 &"
			ssh "$user@$host" "tmux send-keys -t :w0.2 $remote_install_dir/$script_name ENTER"

			sleep 1
			new_pid=$(ssh "$user@$host" "pgrep -f $script_name")
			if [ -n "$new_pid" ]; then
				echo "Successfully relaunched with PID: $new_pid"
			else
				echo "ERROR: Failed to launch script on $host."
			fi
		fi
	done

	sleep 5
done