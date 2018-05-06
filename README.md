# CSDS HW6: CF based Music Recommendation
# Author: Zhifu Xiao (zx2201)

### Part 1

The file **homework6.ipynb** is the notebook file for this part. It contains a bash script to do some data preprocessing. The main function is to generate CF-based music recommendation. The output for 10 favourite products are shown below:

> 50 Cent \
> [unknown] \
> Eminem \
> Red Hot Chili Peppers \
> Nirvana \
> Outkast \
> Linkin Park \
> Green Day \
> Gwen Stefani \
> Beastie Boys

### Part 2

The file **hw6-1.png** and **hw6-2.png** are the outputs from AWS EC2 server. In this part, I firstly used WinSCP to transfer the data from my own computer to the server. Next, I ran the command to do data preprocessing like in part 1, and I then transfer the data files from local to HDFS system. The codes are shown below.

> awk 'NF>=2' artist_alias.txt > artist_alias_new.txt \
> awk '$1 + 0 !=S1 && NF>=2' artist_data.txt > artist_data_new.txt \
> hadoop fs -put artist_alias_new.txt / \
> hadoop fs -put user_artist_data.txt / \
> hadoop fs -put artist_data_new.txt /

Finally, I ran the spark command to execute the py file. **homework6.py** is the python code with some modification on the data input part: I changed the source to HDFS so that AWS could correctly find the data file. The output is shown in the picture, and I use the following command to show the code is actually run on the AWS server. There are some differences between two results, but **Beastie Boys**, **Green Day**, **Nirvana**, **Red Hot Chili Peppers** and **Eminem** appear in both results, so I believe this user might be interested in those artists.

> aws ec2 describe-instances
