# dataengineering2
Git repo for the second assignment of data engineering


# Create compute engine
gcloud compute instances create de2 --project=driven-plexus-325011 --zone=europe-north1-a --machine-type=e2-highmem-16 --network-interface=network-tier=PREMIUM,subnet=default --maintenance-policy=MIGRATE --service-account=963472884911-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --tags=http-server,https-server --create-disk=auto-delete=yes,boot=yes,device-name=lab6,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20210927,mode=rw,size=50,type=projects/driven-plexus-325011/zones/europe-north1-a/diskTypes/pd-balanced --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any

# Open firewall to application
gcloud compute firewall-rules create flask-port --allow tcp:5000
gcloud compute firewall-rules create jupyter-port --allow tcp:8888
gcloud compute firewall-rules create spark-driver-ui-port --allow tcp:4040
gcloud compute firewall-rules create spark-master-port --allow tcp:7077
gcloud compute firewall-rules create spark-master-ui-port --allow tcp:8080
gcloud compute firewall-rules create spark-worker-1-ui-port --allow tcp:8081
gcloud compute firewall-rules create spark-worker-2-ui-port --allow tcp:8082
gcloud compute firewall-rules create zookeeper-port --allow tcp:2181
gcloud compute firewall-rules create kafka-port --allow tcp:9092

# Install docker and docker compose
sudo apt-get update && yes | sudo apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - && sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && sudo apt-get update && yes | sudo apt-get install docker-ce docker-ce-cli containerd.io && sudo curl -L "https://github.com/docker/compose/releases/download/1.28.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose && sudo chmod +x /usr/local/bin/docker-compose

# Clone github repository
git clone https://github.com/IndikaKuma/DE2021.git #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

# Launch application
mkdir notebooks data checkpoint && sudo chmod 777 notebooks data checkpoint && sudo docker-compose up -d --build

# Extract juypter URL from logs
sudo docker logs spark-driver-app 

# Full restart 
sudo docker-compose down --remove-orphans && cd ../ && sudo rm -r notebooks data checkpoint && mkdir notebooks data checkpoint && sudo chmod 777 notebooks data checkpoint && cd dataengineering2 && sudo docker-compose up --build -d

# Delete compute engine
gcloud compute instances delete de2 --zone=europe-north1-a
