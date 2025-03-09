package com.appsdeveloperblog.ws.product.service;

import com.appsdeveloperblog.ws.product.rest.CreateProductRestModel;

public interface ProductService {

    String createProduct(CreateProductRestModel product)  throws Exception;
}

