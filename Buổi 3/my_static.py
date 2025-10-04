class MyStatistic:
    def find_orders_within_range(df, minValue, maxValue):
        # tổng giá trị từng đơn hàng
        order_totals = df.groupby('OrderID').apply(lambda x: (x['UnitPrice'] * x['Quantity'] * (1 - x['Discount'])).sum())
        # lọc đơn hàng trong range
        orders_within_range = order_totals[(order_totals >= minValue) & (order_totals <= maxValue)]
        # danh sách các mã đơn hàng không trùng nhau
        unique_orders = df[df['OrderID'].isin(orders_within_range.index)]['OrderID'].drop_duplicates().tolist()

        return unique_orders
    
    def find_orders_with_totals(df, minValue, maxValue, sorttype=True):
        """
        Tìm đơn hàng trong khoảng giá trị và trả về cả OrderID và tổng giá trị
        
        Args:
            df: DataFrame chứa dữ liệu
            minValue: Giá trị tối thiểu
            maxValue: Giá trị tối đa  
            sorttype: True=tăng dần, False=giảm dần
            
        Returns:
            DataFrame với OrderID và Total đã được sắp xếp
        """
        # tổng giá trị từng đơn hàng
        order_totals = df.groupby('OrderID').apply(lambda x: (x['UnitPrice'] * x['Quantity'] * (1 - x['Discount'])).sum())
        
        # lọc đơn hàng trong range
        orders_within_range = order_totals[(order_totals >= minValue) & (order_totals <= maxValue)]
        
        # sắp xếp theo sorttype
        if sorttype:
            orders_within_range = orders_within_range.sort_values(ascending=True)
        else:
            orders_within_range = orders_within_range.sort_values(ascending=False)
        
        # chuyển thành DataFrame để dễ đọc
        result_df = orders_within_range.reset_index()
        result_df.columns = ['OrderID', 'Sum']
        
        return result_df
    
    
