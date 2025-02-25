# SQLGenPro

In this LLM project,building a user-friendly web application that leverages Large Language Models (LLMs) to convert natural language queries into optimized SQL commands.

![Alt Text](https://projex.gumlet.io/text-2-sql-llm/images/image_(4).png?w=1000&dpr=2.0![image](https://github.com/user-attachments/assets/1b1947ed-b6d5-4088-bddf-a8051153ca4c))

## Files in repositories
1. Data folder contains all the sample data for testing.
2. Articafts folder for the logos used in the streamlit app
3. SRC folder is contains two files for logo creation and main functions used in the program
4. requirements.txt file is for requirements
5. SQLGenPro_Live.py file is the main file contining code for all the streamlit web app
6. authenticator.yml file is for authetication purpose those users only can login to the app
7. env is the .env file where we upload all the envinormental variables.
   
## What will you learn?
1. Learn the basics of LLMs and how they can be leveraged for natural language processing tasks
2. Familiarize yourself with the technologies used in the project: Streamlit, OpenAI, Databricks, and Snowflake
3. Understand how to securely connect and interact with databases on Databricks
4. Learn how to integrate an Open AI API to convert natural language inputs into SQL queries
5. Understand the principles of prompt design and chaining to generate accurate and complex SQL queries
6. Learn how to implement robust error handling to manage SQL generation and execution issues
7. Discover techniques for validating and optimizing SQL queries to enhance performance and reliability
8. Learn how to implement secure user authentication and manage user sessions within a web application
9. Understand how to test and validate complex SQL queries within the application to ensure accuracy and performance
10. Learn how to dynamically generate and render ERD diagrams using LLMs and integrate them into the application
11. Learn how to implement a self-correction feedback loop that allows the application to refine and improve the accuracy of generated SQL queries over time
12. Acquire knowledge of how to configure and deploy web applications securely on AWS EC2, including HTTPS setup


## Project Description:

In today's data-driven world, organizations across industries rely heavily on data to make informed decisions. Data is often stored in relational databases, which require SQL (Structured Query Language) to retrieve and manipulate information. However, writing SQL queries can be challenging for many professionals, especially those without a technical background, such as business analysts, product managers, or executives.

Despite the widespread availability of data, a significant bottleneck remains: the need for SQL expertise to extract meaningful insights. This creates a dependency on specialized teams, such as data analysts or engineers, to write queries and generate reports. As a result, decision-making can be delayed, and productivity hampered, particularly in situations where quick access to data is critical.

## Why a Text-to-SQL Tool is Necessary:

The need for democratizing data access is more pressing than ever. A Text-to-SQL tool bridges this gap by enabling non-technical users to interact with databases using natural language. This reduces the reliance on SQL experts, accelerates decision-making, and promotes a more data-driven culture within organizations.

Moreover, even for experienced coders, writing complex SQL queries can be time-consuming. A tool that can accurately translate natural language into optimized SQL queries can significantly enhance productivity, allowing coders to focus on more strategic tasks rather than spending time on routine query writing.

## How LLMs Are Solving Productivity Problems:

Large Language Models (LLMs) like GPT-4 have shown remarkable capabilities in understanding and generating human-like text. When applied to the task of translating natural language to SQL, LLMs can accurately interpret user intentions, generate complex SQL queries, and even optimize them for performance. This reduces the likelihood of errors, enhances query accuracy, and enables faster data retrieval.

LLMs also contribute to productivity by automating routine tasks, reducing the time spent on query formulation, and enabling even non-technical users to retrieve data independently. This empowers a broader range of professionals to engage with data directly, fostering a more agile and responsive organization.

 

In this project, we will develop a user-friendly Streamlit web application that leverages LLMs to convert natural language queries into SQL commands using Open AI and Databricks.

Check out our previous project, Build a Langchain Streamlit Chatbot for EDA using LLMs, which generates SQL queries to fetch data and Python code to create visualizations, enhancing exploratory data analysis through interactive chatbot capabilities.


## Data Description
The food delivery application database contains detailed records of users, restaurants, menu items, orders, payments, and reviews, capturing every transaction and interaction within the app. This comprehensive dataset, with tables such as Users, Restaurants, Orders, and Reviews, is designed to support the analysis of customer behaviors, service optimization, and overall business performance.

### Tech Stack

Language: Python 3.10
Libraries: Langchain, Langchain-openai, streamlit, Databricks
Model: GPT-4
Cloud Platform: Microsoft Azure and AWS

## Approach
### 1. Virtual Environment and Dependencies:
Create a Python virtual environment and install necessary libraries (Streamlit, OpenAI, LangChain, database connectors).
Configure access to Databricks and Snowflake, including connection setup and testing.
Set up the development environment in VS Code.

### 2. LLM Integration and Database Interaction:
Integrate the LLM via API to convert natural language queries into SQL.
Develop backend modules to execute SQL queries on Databricks/Snowflake and retrieve results.
Implement error handling to manage SQL generation and execution issues.

### 3. Prompt Engineering and SQL Optimization:
Create and refine prompt templates to accurately generate SQL, including complex queries (CTEs, joins).
Implement SQL validation and optimization to ensure generated queries are efficient and correct.

### 4. User Interface and Authentication:
Build an intuitive Streamlit interface for inputting queries, displaying SQL results, and managing query history/favorites. 
Implement user authentication with secure session management and personalized experiences.

### 5. Advanced Query Handling and Feature Integration:
Develop "Quick Analysis," "Deep Dive Analysis," and "Favorites" sections for enhanced user interaction.
Test the application with complex SQL queries to ensure robustness.

### 6. Deployment:
Configure and deploy the application on AWS EC2 with HTTPS for secure access and scalable performance.

<img width="1440" alt="image" src="https://github.com/user-attachments/assets/2fb28c3b-b073-4f01-aa1c-82c92661f5b7" />


<img width="761" alt="Screenshot 2025-02-17 at 1 31 50 AM" src="https://github.com/user-attachments/assets/4c407b43-d4a2-4cfc-9b6c-89e44648cc08" /> 

 

<img width="761" alt="Screenshot 2025-02-17 at 1 34 40 AM" src="https://github.com/user-attachments/assets/13e14bdf-8fdd-4b11-b68c-aa7cf98ffef7" /> 
<img width="761" alt="Screenshot 2025-02-17 at 1 33 56 AM" src="https://github.com/user-attachments/assets/deeb3045-f4fd-4707-9c55-4ade0d514e52" />
<img width="761" alt="Screenshot 2025-02-17 at 1 33 14 AM" src="https://github.com/user-attachments/assets/651ce5de-81a4-4e4d-ad2d-618fa91f112c" />
<img width="761" alt="Screenshot 2025-02-17 at 1 32 53 AM" src="https://github.com/user-attachments/assets/754e92df-6a93-49f0-b23d-92b8839dd8ec" />
<img width="761" alt="Screenshot 2025-02-17 at 1 32 30 AM" src="https://github.com/user-attachments/assets/310bd3db-2aaa-4df9-a0a5-c3b3def1d559" />
<img width="761" alt="Screenshot 2025-02-17 at 1 31 50 AM" src="https://github.com/user-attachments/assets/18805a91-a599-445e-a84e-b2724b7aaf08" />


## Prerequisites
Python 3.10 must be installed.
Ensure you have access to the required datasets and connection strings for databases.

### Virtual Environment Setup Instructions

#### For Mac and Linux:
Open a terminal and navigate to the project directory:

`cd /path/to/SQLGenPro`

Create a virtual environment using venv:


`python3 -m venv venv`


Activate the virtual environment:


`source venv/bin/activate`

Install the required dependencies from the requirements.txt file:


`pip install -r requirements.txt`

#### For Windows:
Open a command prompt and navigate to the project directory:

`cd \path\to\SQLGenPro`

Create a virtual environment using venv:

`python -m venv venv`


Activate the virtual environment:

`.\venv\Scripts\activate`

Install the required dependencies from the requirements.txt file:


`pip install -r requirements.txt`

#### Project Execution
Run the SQLGenPro application using Streamlit:


`streamlit run SQLGenPro.py`

Access the application:

After running the command, the app will open automatically in your default browser.
If not, navigate to the provided local URL (usually http://localhost:8501/) to access the SQLGenPro interface.


```
SQLGenPro

├─ .streamlit
│  └─ config.toml
├─ Langchain_Intro.ipynb
├─ README.md
├─ SQLGenPro.png
├─ SQLGenPro.py
├─ SQLGenPro_Live.py
├─ artifacts/
├─ authenticator.yml
├─ data/
├─ helper.ipynb
├─ requirements.txt
├─ src
│  ├─ add_logo.py
│  └─ utils.py
├─ test.sql
└─ utils.ipynb

```
