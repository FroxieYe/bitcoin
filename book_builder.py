import pandas as pd
import numpy as np
import pqdict
from pqdict import pqdict as pqd
class Node(object):
	def __init__(self, order_id = None, qty = None, ts = None, \
			capture_ts = None, side = None):
		self.order_id = order_id
		self.qty = qty
		self.ts = ts
		self.capture_ts = capture_ts
		self.side = side
		self.next = None

class Book_driver(object):
	def __init__(self, depth = 5):
		self.__bid_record = {}
		self.__ask_record = {}
		self.__bid_pqdict = pqd()
		self.__ask_pqdict = pqd()
		self.__bid_book = []
		self.__ask_book = []
		self.__depth = depth
		self.__error_cnt = 0

	def load(self, df):
		action = df.action.values
		price = df.price.values
		side = df.order_type.values
		capture_ts = df.arttime.values
		ts = df.datetime.values
		qty = df.amount.values
		order_id = df.id.values
		for i in xrange(action.shape[0]):
			if side[i] == 0:
				self.__push_queue(action[i], -1*price[i], \
					self.__bid_pqdict, order_id[i], qty[i], \
					ts[i], capture_ts[i], side[i])
			elif side[i] == 1:
				self.__push_queue(action[i], price[i], \
					self.__ask_pqdict, order_id[i], qty[i], \
					ts[i], capture_ts[i], side[i])
			else:
				raise ValueError("The order type is not recognized %u" %side[i])
			self.__bid_book.append(self.__gen_book(self.__bid_record))
			self.__ask_book.append(self.__gen_book(self.__ask_record))
		print self.__error_cnt
		return self.__bid_book, self.__ask_book

	def __push_queue(self, action, price, pq, order_id, qty, ts, capture_ts, side):
		if action == 0:
			if price not in pq:
				pq[price] = (price, 1, Node(order_id, qty, ts, capture_ts, side))
			else:
				length, listNode = pq[price][1], pq[price][2]
				cur = listNode
				print length
				while cur.next != None:
					cur = cur.next
				cur.next = Node(order_id, qty, ts, capture_ts, side)
				pq[price] = (price, length+1, listNode)
		elif action == 1:
			if price not in pq:
				pq[price] = (price, 1, Node(order_id, qty, ts, capture_ts, side))
			else:
				length, listNode = pq[price][1], pq[price][2]
				cur = listNode
				if length == 1 and cur.order_id == order_id:
					pq[price] = (price, 1, Node(order_id, qty, ts, capture_ts, side))
					return
				node = None
				while cur.next != None:
					if cur.next.order_id == order_id:
						node = cur.next
						if cur.next.next != None:
							cur.next = cur.next.next
						node.qty = qty
						node.side = side
						node.ts = ts
						node.capture_ts = capture_ts
						node.next = None
					cur = cur.next
				cur.next = node
				pq[price] = (price, length, listNode)
		elif action == 2:
			if price in pq:
				length, listNode = pq[price][1], pq[price][2]
				cur = listNode
				if length == 1 and cur.order_id == order_id:
					pq.pop(price)
					return
				while cur.next != None:
					if cur.next.order_id == order_id:
						cur.next = cur.next.next
						break
					cur = cur.next
				pq[price] = (price, length, listNode)

	def __gen_book(self, record):
		lvl = 0
		j = 0
		candidates = pqdict.nsmallest(self.__depth, record)
		res = [[0, 0] for i in xrange(self.__depth)]
		for k,v in candidates:
			cur = v[2]
			qty = 0
			while cur != None:
				qty += cur.qty
			res[j][0] = abs(k)
			res[j][1] = qty
			j += 1
		return res

def tuple_binary_search(A, target1, target2_idx, target2):
	left, right = 0, len(A)-1
	while left <= right:
		mid = left + ((right - left) >> 1)
		if A[mid][0] == target1:
			i = mid + 1
			while i < len(A) and A[i][0] == target1:
				i += 1
			j = mid - 1
			while j >= 0 and A[j][0] == target1:
				j -= 1
			for k in xrange(j+1, i):
				if A[k][target2_idx] == target2:
					return k
		if A[mid][0] < target1:
			left = mid + 1
		else:
			right = mid - 1
	return -1

if __name__ == "__main__":
	md_order = pd.read_csv("/Users/wenshuaiye/Kaggle/bitcoin/data/live_order_1")
	driver = Book_driver(5)
	tmp = driver.load(md_order)
			
