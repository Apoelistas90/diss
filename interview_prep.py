#https://www.interviewcake.com/question/python/stock-price
#Example 1
stock_prices_yesterday = [200, 100, 7, 5, 8, 11, 9]

def get_max_profit(stock_prices_yesterday):
    max_stock_price = stock_prices_yesterday[0]
    min_stock_price = stock_prices_yesterday[0]

    for stock_index in range(1,len(stock_prices_yesterday)): #stock_price in stock_prices_yesterday:

        if max_stock_price < stock_prices_yesterday[stock_index]:
            max_stock_price=stock_prices_yesterday[stock_index]
        if min_stock_price > stock_prices_yesterday[stock_index] and min_stock_price <= max_stock_price:
            min_stock_price=stock_prices_yesterday[stock_index]

        print 'On day #'+str(stock_index) + ', details are: ' + str(stock_prices_yesterday[stock_index]) + '|' + str(max_stock_price) + '|' + str(min_stock_price)
        print '*'

    return max_stock_price - min_stock_price

# stock_prices_yesterday = [200, 100, 7, 5, 8, 11, 9]
def get_max_profit_correct(stock_prices_yesterday):

    min_price = stock_prices_yesterday[0]
    max_profit = stock_prices_yesterday[1] - stock_prices_yesterday[0]

    for index,stock_price in enumerate(stock_prices_yesterday):
        if index==0:
            continue
        potential_profit = stock_price - min_price
        max_profit = max(max_profit,potential_profit)
        min_price = min(min_price,stock_price)
    return max_profit

#print(get_max_profit(stock_prices_yesterday))
#print(get_max_profit_correct(stock_prices_yesterday))


#Example 2
mylist = [1, 7, 3, 4, 1, 0]

def get_products_of_all_ints_except_at_index(alist):
    results = []
    list_len = len(alist)

    for n in range(list_len):
       temp_number = alist[n]
       alist.remove(temp_number)
       #print(alist)
       res = 1
       for num in alist:
           res = res* num

       results.append(res)

       alist.insert(n,temp_number)
       #print(alist)

    print results

#print(get_products_of_all_ints_except_at_index(mylist))



#Example 3

list = [1,2,3,4,5]







