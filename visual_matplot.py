import matplotlib.pyplot as plt

Collections = ['1','2','3','4','5','6']
values = [1,12,34,56,78,89]

plt.subplot(131)
plt.bar(Collections, values)
plt.subplot(132)
plt.scatter(Collections, values)
plt.subplot(133)
plt.plot(Collections, values)
plt.suptitle('Categorical Plotting')
plt.show()