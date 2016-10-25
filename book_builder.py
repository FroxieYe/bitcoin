import pandas as pd
import numpy as np
import os
import pqdict
import csv_reader as cr
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
		self.__bid_pqdict = pqd()
		self.__ask_pqdict = pqd()
		self.__bid_book = []
		self.__ask_book = []
		self.__depth = depth
		self.__capture_ts = []
		self.__error_cnt = 0
		
	def load(self, df, trade):
		action = df.action.values
		price = df.price.values
		side = df.order_type.values
		capture_ts = df.arttime.values
		ts = df.datetime.values
		qty = df.amount.values
		order_id = df.id.values
		trades = {}
		
		tol = 10
		for i, row in trade.iterrows():
			if row.type == 0:
				for j in np.arange(row.timestamp-tol, \
						row.timestamp+tol+1):
					trades[(j, row.buy_order_id)] = \
							(row.price, row.amount)
					trades[(j, row.sell_order_id)] = \
							(row.price, row.amount)
			elif row.type == 1:
				for j in np.arange(row.timestamp-tol, \
						row.timestamp+tol+1):
					trades[(j, row.sell_order_id)] = \
							(row.price, row.amount)
					trades[(j, row.buy_order_id)] = \
							(row.price, row.amount)
		
		for i in xrange(action.shape[0]):
			if (ts[i], order_id[i]) in trades:
				#if qty[i] == trades[(ts[i], order_id[i])][1]:
				#print "Market Order with price: ", \
				#		trades[(ts[i], order_id[i])][0]
				continue
			if side[i] == 0:
				self.__push_queue(action[i], -1*price[i], \
					self.__bid_pqdict, order_id[i], qty[i], \
					ts[i], capture_ts[i], side[i])
			elif side[i] == 1:
				self.__push_queue(action[i], price[i], \
					self.__ask_pqdict, order_id[i], qty[i], \
					ts[i], capture_ts[i], side[i])
			else:
				raise ValueError("The order type is not recognized %u" \
					%side[i])
			self.__capture_ts.append(capture_ts[i])
			self.__bid_book.append(self.__gen_book(self.__bid_pqdict))
			self.__ask_book.append(self.__gen_book(self.__ask_pqdict))
		#print self.__bid_pqdict
		#print self.__ask_pqdict
		return self.__bid_book, self.__ask_book, self.__capture_ts

	def __push_queue(self, action, price, pq, order_id, qty, ts, \
			capture_ts, side):
		if action == 0:
			if price not in pq:
				pq[price] = (price, 1, Node(order_id, qty, ts, \
						capture_ts, side))
				cur = pq[price][2]
				while cur != None:
					cur = cur.next	
			else:
				length, listNode = pq[price][1], pq[price][2]
				cur = listNode
				while cur.next != None:
					cur = cur.next
				cur.next = Node(order_id, qty, ts, capture_ts, side)
				pq[price] = (price, length+1, listNode)
		elif action == 1:
			if price not in pq:
				pq[price] = (price, 1, Node(order_id, qty, ts, \
						capture_ts, side))
			else:
				length, listNode = pq[price][1], pq[price][2]
				cur = listNode
				'''
				if length == 1 and cur.order_id == order_id:
					pq[price] = (price, 1, Node(order_id, qty, ts, \
							capture_ts, side))
					return
				'''
				node = None
				dummy = Node()
				dummy.next = listNode
				cur = dummy
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
				if cur.order_id == order_id:
					cur.qty = qty
					cur.ts = ts,
					cur.capture_ts = capture_ts
				else:
					cur.next = node
				pq[price] = (price, length, dummy.next)
		elif action == 2:
			if price in pq:
				length, listNode = pq[price][1], pq[price][2]
				dummy = Node()
				dummy.next = listNode
				cur = dummy
				while cur.next != None:
					if cur.next.order_id == order_id:
						cur.next = cur.next.next
						length -= 1
						break
					cur = cur.next
				if length == 0:
					pq.pop(price)
					return
				pq[price] = (price, length, dummy.next)
					
	def __gen_book(self, record):
		lvl = 0
		j = 0
		candidates = pqdict.nsmallest(self.__depth, record)
		res = [0 for i in xrange(2*self.__depth)]
		for k in candidates:
			v = record[k]
			cur = v[2]
			qty = 0
			while cur != None:
				qty += cur.qty
				cur = cur.next
			res[j] = abs(k)
			res[j+1] = qty
			j += 2
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
	path = "/Users/wenshuaiye/Kaggle/bitcoin/data/20161018_"
	md_order = pd.DataFrame()
	trade = cr.load(5,8, "live_trade_")
	for i in xrange(12, 15):
		tmp = pd.read_csv(os.path.join(path, "live_order_" + str(i)))
		md_order = pd.concat([md_order, tmp], copy = False)
	#md_order = md_order[(md_order.price < 650) & (md_order.price > 590)]
	driver = Book_driver(5)
	tmp = driver.load(md_order, trade)
	bid = pd.DataFrame(tmp[0])
	ask = pd.DataFrame(tmp[1])
	ts = tmp[2]
