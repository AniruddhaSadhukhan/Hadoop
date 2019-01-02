#!/bin/sh
gnome-terminal -x bash -c "echo edureka | sudo -S  mount -t vboxsf SharedHadoop /home/edureka/Desktop/SharedHadoop;exit; exec bash"

