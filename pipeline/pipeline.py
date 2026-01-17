import sys
import pandas as pd 
print("Printing sys.argv")
print(sys.argv)

# Remember, whatever received from terminal is alway in the form of text
# So, we have to convert it intopip integer type explicitly

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")



"""




# import sys

# day = int(sys.argv[1])

# print(f"This program is running for day {day}")
# print(f"This program is running for day {sys.argv[2]}")
# print(f"This program is running for day {sys.argv[3]}")
# print(f"This program is running for day {sys.argv[4]}")

# sys.argv[0] is alway the name of the file. In our case, it is pipeline.python

# sys.argv[1] is the first argument passed from terminal to the program

# ls
# pwd

# mkdir pipeline

# cd pipeline

# touch pipeline.py



# Terminal:
# # python pipeline.py 12 123 1234 12345

# Then, we had to work through pandas

# The system here, does not have pandas

# Just to make sure that we have a local environment that does not conflict
# with the global environments, we will set up a local environment

# We will create a dot venv environment

# To create the .venv environment, a few steps need to be followed

# Firstly, make sure that you are in the right space

# Do cd pipeline to make sure that you are in the pipeline folder 
# which consists of the code

# Then, type in the following few commands in the terminal:



pip install uv
This command will install uv

uv init --python=3.13
This will initiate the python project with uv

Now that we have a virtual environment, we can download the things here

which python
which python - V
This will give the python version of the global environment

uv run which python
uv run python - V
These commands, however, will give the python version of the local one
Here, we expect to obtain something like python:3.13.11

Then, we can install numpy and pyarrow

uv add pyarrow pandas
This command will add pandas and pyarrow

uv run python pipeline.py 12
This command will run the thing and give us the required results.

Here, so far, we have created a virtual environment to do the things.
We are running things in our virtual environment

This process gives us few more files. 
those files include .python-version, pyproject.toml, and so on
Later, if we decide to use uv to create a docker image, we might have to copy these
files to our docker image.

Now, here, we are looking at creating a docker image.

So, firstly make sure you are inside the pipeline folder

Then, create a file called Dockerfile

Type in:

FROM python=3.13.11-slim
This creates a docker image of a light linux with python-13 already installed on it

run pip install pyarrow

create a working directory
WORKDIR /app

copy pipeline.py pipeline.py

run from the terminal or 
ENTRYPOINT ["python", "pipeline.py"]

These code would create a docker image of a light linux with python 
already installed on it and then we install pyarrow and pandas on it

then we create a working directory and inside that, we would copy 
our pipeline.py

then maybe we would write an ENTRYPOINT ["python", "pipeline.py"] code or maybe not
because we can run it from the terminal any ways

To run it from the terminal, we need to build it, then run it and then go inside it 
to make the necessary changes and so on.

The commands for the terminal are:
docker build -t test:pandas
Here, we are naming it as pandas for fun

Then, we run it
docker run test:pandas

This takes to the entrypoint and we are basically inside that image and now we can run things
docker run -it --entrypoint = bash --rm test:pandas

We can also create that parquet file inside this docker image

Just in case, if we want to run it inside our docker image but save the required files and 
the things that we obtain from running into our laptop, we can do something like:
docker run -it --entrypoint = bash --rm v $(pwd):/app test:pandas
This code actually not only helps us reach out to the docker image, it also allows us to
save whatever is obatained by running in that code. If a .parquet file is created, it can be 
obtained to our pwd into our laptop from what it is running at /app/test/pandas

Noticed that docker run test:pandas actually gives us the results from running the file
pipeline.py into the terminal, and wonder why?
It is because in our code, we have ENTRYPOINT [] or CMD = []
So, this tells docker build to run this file pipeline.py

docker run -it --rm -v $(pwd):/app test:pandas 15
This command will run the pipeline.py as well because it has ENTRYPOINT [] 
and then, it will also pass sys.argv[1] = 15 to the files that are on /app test:pandas
It does not matter how many files there are, it will pass 15 as sys.argv[1]
Then, it will also create a bridge between our computer's current working dir and 
/app test:pandas. After running if abc.parquet file is obtained, it is directly saved to
our computer's current directory.



Now, what if we have to create a docker image of uv

FROM python=3.13.12-slim

COPY official-uv-docker-image /uv /bin

Set working directory
WORKDIR /app

Then, also set the ENV path
ENV PATH = "/app /.venv/bin: $PATH"

Importing linux with python, copying the uv image, setting the workdir
Then, setting the ENV path

Then, copy all the files created in the process
COPY "uv.lock" "pyproject.toml" ".python-version" ./

Run this command to install dependencies from the lock file:
RUN uv sync --locked

COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]


Here, we used FROM for creating a small linux with python
then, we copied official uv image to /uv /bin folder
then we created a working dir WORKDIR /app
then we set the ENV PATH = "app/.venv /bin: $PATH"
then we copied the files created by uv in our laptop
we did RUN uv sync --locked
we copied pipeline.py
then we set the entry point

Running the similar terminal commands will do the job:
docker build -t test:pandas
docker run test:pandas

docker run -it --rm test:pandas 12 


"""