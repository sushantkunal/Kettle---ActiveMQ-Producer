
package com.athi.kettle.plugin.activemq;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaString;

/**
 * Describes a single field in a queue input
 *
 * @author Sushant Kunal
 * @since 25-11-2016
 *
 */
public class QueueMessageField implements Cloneable {
	@Injection(name = "OUTPUT_FIELDNAME", group = "MESSAGE_FIELDS")
	private String name;

	@Injection(name = "OUTPUT_TYPE", group = "MESSAGE_FIELDS")
	private int type;

	@Injection(name = "OUTPUT_FORMAT", group = "MESSAGE_FIELDS")
	private String format;

	@Injection(name = "OUTPUT_LENGTH", group = "MESSAGE_FIELDS")
	private int length = -1;

	@Injection(name = "OUTPUT_PRECISION", group = "MESSAGE_FIELDS")
	private int precision = -1;

	@Injection(name = "OUTPUT_CURRENCY", group = "MESSAGE_FIELDS")
	private String currencySymbol;

	@Injection(name = "OUTPUT_DECIMAL", group = "MESSAGE_FIELDS")
	private String decimalSymbol;

	@Injection(name = "OUTPUT_GROUP", group = "MESSAGE_FIELDS")
	private String groupingSymbol;

	@Injection(name = "OUTPUT_NULL", group = "MESSAGE_FIELDS")
	private String nullString;

	@Injection(name = "OUTPUT_TRIM", group = "MESSAGE_FIELDS")
	private int trimType;

	public QueueMessageField(String name, int type, String format, int length, int precision, String currencySymbol,
			String decimalSymbol, String groupSymbol, String nullString) {
		this.name = name;
		this.type = type;
		this.format = format;
		this.length = length;
		this.precision = precision;
		this.currencySymbol = currencySymbol;
		this.decimalSymbol = decimalSymbol;
		this.groupingSymbol = groupSymbol;
		this.nullString = nullString;
	}

	public QueueMessageField() {
	}

	public int compare(Object obj) {
		QueueMessageField field = (QueueMessageField) obj;
		return name.compareTo(field.getName());
	}

	public boolean equal(Object obj) {
		QueueMessageField field = (QueueMessageField) obj;
		return name.equals(field.getName());
	}

	@Override
	public Object clone() {
		try {
			Object retval = super.clone();
			return retval;
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public String getName() {
		return name;
	}

	public void setName(String fieldname) {
		this.name = fieldname;
	}

	public int getType() {
		return type;
	}

	public String getTypeDesc() {
		return ValueMetaFactory.getValueMetaName(type);
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setType(String typeDesc) {
		this.type = ValueMetaFactory.getIdForValueMeta(typeDesc);
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getGroupingSymbol() {
		return groupingSymbol;
	}

	public void setGroupingSymbol(String group_symbol) {
		this.groupingSymbol = group_symbol;
	}

	public String getDecimalSymbol() {
		return decimalSymbol;
	}

	public void setDecimalSymbol(String decimal_symbol) {
		this.decimalSymbol = decimal_symbol;
	}

	public String getCurrencySymbol() {
		return currencySymbol;
	}

	public void setCurrencySymbol(String currency_symbol) {
		this.currencySymbol = currency_symbol;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public String getNullString() {
		return nullString;
	}

	public void setNullString(String null_string) {
		this.nullString = null_string;
	}

	@Override
	public String toString() {
		return name + ":" + getTypeDesc();
	}

	public int getTrimType() {
		return trimType;
	}

	public void setTrimType(int trimType) {
		this.trimType = trimType;
	}

	public void setTrimTypeByDesc(String value) {
		this.trimType = ValueMetaString.getTrimTypeByDesc(value);
	}

	public String getTrimTypeCode() {
		return ValueMetaString.getTrimTypeCode(trimType);
	}

	public String getTrimTypeDesc() {
		return ValueMetaString.getTrimTypeDesc(trimType);
	}
}
