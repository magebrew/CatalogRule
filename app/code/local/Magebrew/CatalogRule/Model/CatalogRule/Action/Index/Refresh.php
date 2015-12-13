<?php

class Magebrew_CatalogRule_Model_CatalogRule_Action_Index_Refresh extends Mage_CatalogRule_Model_Action_Index_Refresh
{
    /**
     * Create temporary table
     */
    protected function _createTemporaryTable()
    {
        $this->_connection->dropTemporaryTable($this->_getTemporaryTable());
        $table = $this->_connection->newTable($this->_getTemporaryTable())
            ->addColumn(
                'grouped_id',
                Varien_Db_Ddl_Table::TYPE_VARCHAR,
                80,
                array(),
                'Grouped ID'
            )
            ->addColumn(
                'product_id',
                Varien_Db_Ddl_Table::TYPE_INTEGER,
                null,
                array(
                    'unsigned' => true
                ),
                'Product ID'
            )
            ->addColumn(
                'customer_group_id',
                Varien_Db_Ddl_Table::TYPE_SMALLINT,
                5,
                array(
                    'unsigned' => true
                ),
                'Customer Group ID'
            )
            ->addColumn(
                'from_date',
                Varien_Db_Ddl_Table::TYPE_DATE,
                null,
                array(),
                'From Date'
            )
            ->addColumn(
                'to_date',
                Varien_Db_Ddl_Table::TYPE_DATE,
                null,
                array(),
                'To Date'
            )
            ->addColumn(
                'action_amount',
                Varien_Db_Ddl_Table::TYPE_DECIMAL,
                '12,4',
                array(),
                'Action Amount'
            )
            ->addColumn(
                'action_operator',
                Varien_Db_Ddl_Table::TYPE_VARCHAR,
                10,
                array(),
                'Action Operator'
            )
            ->addColumn(
                'action_stop',
                Varien_Db_Ddl_Table::TYPE_SMALLINT,
                6,
                array(),
                'Action Stop'
            )
            ->addColumn(
                'sort_order',
                Varien_Db_Ddl_Table::TYPE_INTEGER,
                10,
                array(
                    'unsigned' => true
                ),
                'Sort Order'
            )
            ->addColumn(
                'price',
                Varien_Db_Ddl_Table::TYPE_DECIMAL,
                '12,4',
                array(),
                'Product Price'
            )

            //magebrew: add Cost column to temporary table
            ->addColumn(
                'cost',
                Varien_Db_Ddl_Table::TYPE_DECIMAL,
                '12,4',
                array(),
                'Product Cost'
            )
            ->addColumn(
                'rule_product_id',
                Varien_Db_Ddl_Table::TYPE_INTEGER,
                null,
                array(
                    'unsigned' => true
                ),
                'Rule Product ID'
            )
            ->addColumn(
                'from_time',
                Varien_Db_Ddl_Table::TYPE_INTEGER,
                null,
                array(
                    'unsigned' => true,
                    'nullable' => true,
                    'default' => 0,
                ),
                'From Time'
            )
            ->addColumn(
                'to_time',
                Varien_Db_Ddl_Table::TYPE_INTEGER,
                null,
                array(
                    'unsigned' => true,
                    'nullable' => true,
                    'default' => 0,
                ),
                'To Time'
            )
            ->addIndex(
                $this->_connection->getIndexName($this->_getTemporaryTable(), 'grouped_id'),
                array('grouped_id')
            )
            ->setComment('CatalogRule Price Temporary Table');
        $this->_connection->createTemporaryTable($table);
    }

    /**
     * Prepare temporary data
     *
     * @param Mage_Core_Model_Website $website
     * @return Varien_Db_Select
     */
    protected function _prepareTemporarySelect(Mage_Core_Model_Website $website)
    {
        /** @var $catalogFlatHelper Mage_Catalog_Helper_Product_Flat */
        $catalogFlatHelper = $this->_factory->getHelper('catalog/product_flat');

        /** @var $eavConfig Mage_Eav_Model_Config */
        $eavConfig = $this->_factory->getSingleton('eav/config');
        $priceAttribute = $eavConfig->getAttribute(Mage_Catalog_Model_Product::ENTITY, 'price');
        //magebrew: get Cost attribute
        $costAttribute = $eavConfig->getAttribute(Mage_Catalog_Model_Product::ENTITY, 'cost');

        $select = $this->_connection->select()
            ->from(
                array('rp' => $this->_resource->getTable('catalogrule/rule_product')),
                array()
            )
            ->joinInner(
                array('r' => $this->_resource->getTable('catalogrule/rule')),
                'r.rule_id = rp.rule_id',
                array()
            )
            ->where('rp.website_id = ?', $website->getId())
            ->order(
                array('rp.product_id', 'rp.customer_group_id', 'rp.sort_order', 'rp.rule_product_id')
            )
            ->joinLeft(
                array(
                    'pg' => $this->_resource->getTable('catalog/product_attribute_group_price')
                ),
                'pg.entity_id = rp.product_id AND pg.customer_group_id = rp.customer_group_id'
                . ' AND pg.website_id = rp.website_id',
                array()
            )
            ->joinLeft(
                array(
                    'pgd' => $this->_resource->getTable('catalog/product_attribute_group_price')
                ),
                'pgd.entity_id = rp.product_id AND pgd.customer_group_id = rp.customer_group_id'
                . ' AND pgd.website_id = 0',
                array()
            );

        $storeId = $website->getDefaultStore()->getId();

        if ($catalogFlatHelper->isEnabled() && $storeId && $catalogFlatHelper->isBuilt($storeId)) {
            $select->joinInner(
                array('p' => $this->_resource->getTable('catalog/product_flat') . '_' . $storeId),
                'p.entity_id = rp.product_id',
                array()
            );
            $priceColumn = $this->_connection->getIfNullSql(
                $this->_connection->getIfNullSql(
                    'pg.value',
                    'pgd.value'
                ),
                'p.price'
            );
            //magebrew: Cost column select for Flat catalog
            $costColumn = $this->_connection->getIfNullSql(
                'p.cost',
                0
            );
        } else {
            $select->joinInner(
                array(
                    'pd' => $this->_resource->getTable(array('catalog/product', $priceAttribute->getBackendType()))
                ),
                'pd.entity_id = rp.product_id AND pd.store_id = 0 AND pd.attribute_id = '
                . $priceAttribute->getId(),
                array()
            )
                ->joinLeft(
                    array(
                        'p' => $this->_resource->getTable(array('catalog/product', $priceAttribute->getBackendType()))
                    ),
                    'p.entity_id = rp.product_id AND p.store_id = ' . $storeId
                    . ' AND p.attribute_id = pd.attribute_id',
                    array()
                )

                //magebrew: join cost attribute table
                ->joinLeft(
                    array(
                        'pc' => $this->_resource->getTable(array('catalog/product', $costAttribute->getBackendType()))
                    ),
                    'pc.entity_id = rp.product_id AND pc.store_id = 0 AND pc.attribute_id = '
                    . $costAttribute->getId(),
                    array()
                );
            $priceColumn = $this->_connection->getIfNullSql(
                $this->_connection->getIfNullSql(
                    'pg.value',
                    'pgd.value'
                ),
                $this->_connection->getIfNullSql(
                    'p.value',
                    'pd.value'
                )
            );

            //magebrew: Cost column select for Eav catalog
            $costColumn = $this->_connection->getIfNullSql(
                'pc.value',
                0
            );
        }

        $select->columns(
            array(
                'grouped_id' => $this->_connection->getConcatSql(
                    array('rp.product_id', 'rp.customer_group_id'),
                    '-'
                ),
                'product_id' => 'rp.product_id',
                'customer_group_id' => 'rp.customer_group_id',
                'from_date' => 'r.from_date',
                'to_date' => 'r.to_date',
                'action_amount' => 'rp.action_amount',
                'action_operator' => 'rp.action_operator',
                'action_stop' => 'rp.action_stop',
                'sort_order' => 'rp.sort_order',
                'price' => $priceColumn,
                'cost' => $costColumn, //magebrew: add Cost column to select
                'rule_product_id' => 'rp.rule_product_id',
                'from_time' => 'rp.from_time',
                'to_time' => 'rp.to_time'
            )
        );

        return $select;
    }


    /**
     * Prepare price column
     *
     * @return Zend_Db_Expr
     */
    protected function _calculatePrice()
    {
        $toPercent = $this->_connection->quote('to_percent');
        $byPercent = $this->_connection->quote('by_percent');
        $toFixed = $this->_connection->quote('to_fixed');
        $byFixed = $this->_connection->quote('by_fixed');
        $nA = $this->_connection->quote('N/A');

        //magebrew: this is used below in CaseSql statement
        $toCostPercent = $this->_connection->quote('to_cost');

        return $this->_connection->getCaseSql(
            '',
            array(
                $this->_connection->getIfNullSql(
                    new Zend_Db_Expr('@group_id'), $nA
                ) . ' != cppt.grouped_id' =>
                    '@price := ' . $this->_connection->getCaseSql(
                        $this->_connection->quoteIdentifier('cppt.action_operator'),
                        array(
                            $toPercent => new Zend_Db_Expr('cppt.price * cppt.action_amount/100'),
                            $byPercent => new Zend_Db_Expr('cppt.price * (1 - cppt.action_amount/100)'),
                            $toFixed => $this->_connection->getCheckSql(
                                new Zend_Db_Expr('cppt.action_amount < cppt.price'),
                                new Zend_Db_Expr('cppt.action_amount'),
                                new Zend_Db_Expr('cppt.price')
                            ),
                            $byFixed => $this->_connection->getCheckSql(
                                new Zend_Db_Expr('0 > cppt.price - cppt.action_amount'),
                                new Zend_Db_Expr('0'),
                                new Zend_Db_Expr('cppt.price - cppt.action_amount')
                            ),

                            //magebrew: calculation formula for price
                            $toCostPercent => new Zend_Db_Expr('cppt.cost * cppt.action_amount/100'),
                        )
                    ),
                $this->_connection->getIfNullSql(
                    new Zend_Db_Expr('@group_id'), $nA
                ) . ' = cppt.grouped_id AND '
                . $this->_connection->getIfNullSql(
                    new Zend_Db_Expr('@action_stop'),
                    new Zend_Db_Expr(0)
                ) . ' = 0' => '@price := ' . $this->_connection->getCaseSql(
                        $this->_connection->quoteIdentifier('cppt.action_operator'),
                        array(
                            $toPercent => new Zend_Db_Expr('@price * cppt.action_amount/100'),
                            $byPercent => new Zend_Db_Expr('@price * (1 - cppt.action_amount/100)'),
                            $toFixed => $this->_connection->getCheckSql(
                                new Zend_Db_Expr('cppt.action_amount < @price'),
                                new Zend_Db_Expr('cppt.action_amount'),
                                new Zend_Db_Expr('@price')
                            ),
                            $byFixed => $this->_connection->getCheckSql(
                                new Zend_Db_Expr('0 > @price - cppt.action_amount'),
                                new Zend_Db_Expr('0'),
                                new Zend_Db_Expr('@price - cppt.action_amount')
                            ),
                        )
                    )
            ),
            '@price := @price'
        );
    }
}