package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;

interface CompositeExpressionNode {
    ColumnType type();
    Number value(ResultFrame frame, int row);
}
