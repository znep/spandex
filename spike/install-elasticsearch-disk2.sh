sudo fdisk /dev/xvdb
# n p 1 <enter> +60G
# n p 2 <enter> +10G
# n p 3 <enter> <enter>
# w

sudo mkfs -t ext4 /dev/xvdb1 
sudo mkfs -t ext4 /dev/xvdb2 
sudo mkfs -t ext4 /dev/xvdb3 

sudo mkdir /var/lib/elasticsearch
sudo mkdir /var/log/elasticsearch
sudo mkdir /tmp/elasticsearch

sudo vi /etc/fstab
# /dev/xvdb1              /var/lib/elasticsearch  ext4    defaults        0 2
# /dev/xvdb2              /var/log/elasticsearch  ext4    defaults        0 2
# /dev/xvdb3              /tmp/elasticsearch      ext4    defaults        0 2

sudo mount -a

