## How to - jar
- mvn clean install -> result will be in the jar directory

## How to - docker
- Clone repo
- Create logs dir, put log file
- sudo docker-compose up -d
- sudo docker exec -it parser_master_1 /bin/bash
- cd /shell -> standalone.sh or yarn.sh
- Program results are stored in /1/part-00000, /2/part-00000 and /3/part-00000 (use hadoop fs)