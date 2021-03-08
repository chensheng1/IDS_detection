# -*- coding: utf-8 -*-



import pickle
import math
import random

# 初始化随机种子
random.seed(0)

# 创建矩阵类
class createMatrix(list):

    def __init__(self, i, j, fill = 0.0):

        for index in range(i):
            self.append([fill] * j)

# 误差逆传播神经网络类
class BPNeuralNetworks:

	# sigmoid 函数
	@staticmethod
	def sigmoid(x):
		return 1 / (1 + math.exp(-x))

	# 生成随机数：a <= rand < b
	@staticmethod
	def rand(a, b):
		return (b - a) * random.random() + a

	# 类初始化
	def __init__(self, d, l, q):
		
		self.i = d  # 输入层节点数
		self.j = l  # 隐藏层节点数
		self.k = q  # 输出层节点数

		# 储存节点数据
		self.value_i = [1.0] * self.i
		self.value_j = [1.0] * self.j
		self.value_k = [1.0] * self.k
		
		# 创建权重矩阵
		self.weight_a = createMatrix(self.i, self.j) # 输入层 -> 隐藏层
		self.weight_b = createMatrix(self.j, self.k) # 隐藏层 -> 输出层

		# 记录上一次权重矩阵（冲量用）
		self.impulse_a = createMatrix(self.i, self.j)
		self.impulse_b = createMatrix(self.j, self.k)

		# 初始化权重
		for i in range(self.i):
			for j in range(self.j):
				self.weight_a[i][j] = self.rand(-0.2, 0.2)
		for j in range(self.j):
			for k in range(self.k):
				self.weight_b[j][k] = self.rand(-2.0, 2.0)

		# 阈值
		self.threshold_j = [1.0] * self.j
		self.threshold_k = [1.0] * self.k

		# 误差向量
		self.deltas = [0.0] * self.j

	# 前馈函数
	# @param x 输入样本
	def feedForward(self, x):

		if len(x) != self.i: raise ValueError('输入层节点数错误！')

		# 装载输入数据
		for i in range(self.i):
			self.value_i[i] = self.sigmoid(x[i])

		# 计算隐藏层
		for j in range(self.j):
			value = 0.0
			for i in range(self.i):
				value += self.value_i[i] * self.weight_a[i][j]
			value -= self.threshold_j[j] # 减去阈值
			self.value_j[j] = self.sigmoid(value)

		# 计算输出层
		for k in range(self.k):
			value = 0.0
			for j in range(self.j):
				value += self.value_j[j] * self.weight_b[j][k]
			value -= self.threshold_k[k] # 减去阈值
			self.value_k[k] = self.sigmoid(value)
		return self.value_k

	# 反馈函数
	# @param y    输出样本
	# @param eta  学习率
	# @param n    冲量系数，默认不使用
	def backPropagate(self, y, eta = 0.5, n = 0.0):

		if len(y) != self.k: raise ValueError('输出层节点数错误！')

		# 更新隐藏层 -> 输出层的权重与阈值
		for j in range(self.j):
			self.deltas[j] = 0.0
			for k in range(self.k):
				# 计算误差更新
				delta = self.value_k[k] * (y[k] - self.value_k[k]) * (1 - self.value_k[k])
				# 更新权重
				self.weight_b[j][k] += eta * self.value_j[j] * delta # 更新权重
				self.threshold_k[k] -= eta * delta                   # 更新阈值
				# 如果具有冲量项
				if n > 0:
					self.weight_b[j][k] += n * self.impulse_b[j][k]  # 更新权重
				# 更新冲量项
				self.impulse_b[j][k] = eta * self.value_j[j] * delta + n * self.impulse_b[j][k] 
				self.deltas[j]      += self.weight_b[j][k] * delta   # 累积

		# 更新输入层 -> 隐藏层的权重与阈值
		for i in range(self.i):
			for j in range(self.j):
				# 计算误差更新
				delta = self.value_j[j] * (1 - self.value_j[j]) * self.deltas[j]
				# 更新权重
				self.weight_a[i][j] += eta * self.value_i[i] * delta # 更新权重
				self.threshold_j[j] -= eta * delta                   # 更新阈值
				# 如果具有冲量项
				if n > 0:
					self.weight_a[i][j] += n * self.impulse_a[i][j]  # 更新权重
				# 更新冲量项
				self.impulse_a[i][j] = eta * self.value_i[i] * delta + n * self.impulse_a[i][j]

		# 返回误差
		error = 0.0
		for k in range(self.k):
			error += 0.5 * (y[k] - self.value_k[k]) ** 2
		return error


	# 测试函数
	# @param data    输入样本
	def caclulate(self, data):
		list=[]
		for x in data:
			result = self.feedForward(x[1])
			list.append([x[0],result])
		return list

	# 训练函数
	# @param data       输入样本
	# @param iterations 迭代次数
	# @param eta        学习率
	# @param n          冲量系数
	def train(self, data, iterations = 700, eta = 0.5, n = 0.8):
		pre = 0.0 # 记录上次迭代的误差
		list=[]
		for index in range(iterations):
			error = 0.0
			for x in data:
				self.feedForward(x[0])                    # 前馈
				error += self.backPropagate(x[1], eta, n) # 反馈
			if error<=0.001:
				break
			else:
				#if index % 100 == 0:
				#print('error %-.5f' % mse)# 每一百次迭代输出误差
				list.append(error)
				if pre != 0.0 and error != 0.0:
					eta = eta * float(error / pre) # 自适应学习率
				pre = error
		#print(list)

