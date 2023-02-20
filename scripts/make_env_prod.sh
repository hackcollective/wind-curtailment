# this scripts makes the file "env/.env.prod" and uses DB_PASSWORD from environmental variables
echo "Making env/.env.prod file"
echo "DB_IP=34.88.31.153" >> env/.env.prod
echo "DB_USERNAME=postgres" >> env/.env.prod
echo "DB_PASSWORD=${DB_PASSWORD}" >> env/.env.prod
echo "HOST=/cloudsql/wind-curtailment:europe-north1" >> env/.env.prod
echo "CLOUD_SQL_INSTANCE=windcurtailment" >> env/.env.prod

