CheatSheet :  
https://www.postgresqltutorial.com/postgresql-cheat-sheet/  


Inspect containers ID :  
docker container ls  
  

Inspect volumes on container ID:    
docker inspect -f '{{ .Mounts }}' 24b911a4412a  
  
Remove docker volume:   
docker volume rm a32e01c6bf07debbae16db152ff1c3b051af082d566c582eacc20d7fe5b3314b  
  
  
kafka-topics --list --bootstrap-server localhost:9092      
kafka-console-consumer --topic stations-jdbc  --bootstrap-server localhost:9092 --from-beginning   


