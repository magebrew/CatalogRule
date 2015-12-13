<?php

class Magebrew_CatalogRule_Block_Adminhtml_Promo_Catalog_Edit_Tab_Actions extends Mage_Adminhtml_Block_Promo_Catalog_Edit_Tab_Actions
{
    const COST_PERCENT_OPERATOR = 'to_cost';

    protected function _prepareForm()
    {
        parent::_prepareForm();
        $form = $this->getForm();

        /** @var Varien_Data_Form_Element_Fieldset $fieldset */
        $fieldset = $form->getElement('action_fieldset');
        foreach ($fieldset->getElements() as $element) {
            if ($element->getName() == 'simple_action') {
                $element->setOptions($element['options'] + array(self::COST_PERCENT_OPERATOR => 'To Percentage of the Cost'));
                $values = $element->getValues();
                $values[] = array('value' => self::COST_PERCENT_OPERATOR, 'label' => 'To Percentage of the Cost');
                $element->setValues($values);
            }
        }
        return $this;
    }
}