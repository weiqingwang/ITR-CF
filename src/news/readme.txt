ratingProcess:保证rating中出现的电影有15个以上的tag；
ratingProcess2：保证rating中出现的用户评过300部以上电影；
ratingProcess3：划分训练集和测试集，每个用户随机挑30部电影作为测试集。我们可以不用执行，但是需要明白测试集的划分标准。
tagProcess：过滤tag，保证每个tag至少被两个用户使用，至少用于五部电影；
tagProcess2：处理tag，保证其中出现的电影要求有15个以上tag
