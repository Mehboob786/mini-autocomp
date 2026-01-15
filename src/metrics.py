def estimate_query_latency(file_count, data_size):
    alpha = 0.5
    beta = 0.01
    return alpha * file_count + beta * data_size
