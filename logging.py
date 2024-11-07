import pandas as pd

# Read and clean up duplicates
df = pd.read_csv('logs.csv')
df.drop_duplicates(inplace=True)
df.to_csv('logs_cleaned.csv', mode='w', index=False, header=False)

# Group by 'Subreddit', count entries, and sum 'numComments'
metrics_df = df.groupby('Subreddit').agg(numPosts=('postID', 'size'), numComments=('numComments', 'sum')).reset_index()

# Calculate the sum of numPosts and numComments columns
total_posts = metrics_df['numPosts'].sum()
total_comments = metrics_df['numComments'].sum()

## Append a row for totals
totals_row = pd.DataFrame([['Total', total_posts, total_comments]], columns=metrics_df.columns)
# metrics_df = pd.concat([metrics_df, totals_row], ignore_index=True)

# Print the result nicely formatted
print(metrics_df.to_string(index=False))
print("------------------------------------------")
print(totals_row.to_string(index=False, header=False))
print("------------------------------------------")
print(f"total data points: {total_posts + total_comments}")
