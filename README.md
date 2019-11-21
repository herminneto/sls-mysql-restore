# Installation Guide


### 1. serverless dependency install

    npm install

### 2. virtual environment install
    pip install -r requirements.txt

### 3. Create RDS database on AWS
Create a DB claster on AWS called **onetdb**. you have to get DB arn from configuration.
Create secret ARN for the database.  you have to get DB secrete ARN from the configuration.     
### 4. config file
Copy **config.json.example** to **config.json**.
Copy DB ARN and DB secret ARN to the **config.json** file.
### 5. deployment 

     serverless deploy -v --stage dev
