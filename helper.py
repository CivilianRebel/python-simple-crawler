# didnt work so i used numpy instead
# deprecated
def split(arr, n_chunks):
    for i in range(0, len(arr), n_chunks):
        yield arr[i:i + n_chunks]
