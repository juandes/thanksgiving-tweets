A = LOAD 'tweetsdata/' USING PigStorage(); 
set default_parallel 1;
store A into 'thanksgiving_tweets/' using PigStorage();