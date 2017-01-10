/*
 * Created on 18-mei-2003
 *
 */

package com.athi.kettle.plugin.activemq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ActiveMQProducerDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = ActiveMQProducerMeta.class;
	private ActiveMQProducerMeta input;
	private ValueMetaAndData value;

	private CTabFolder wTabFolder;
	private CTabItem wContentTab;
	private CTabItem wFieldsTab;

	private FormData fdContentComp, fdFieldsComp, fdTabFolder;

	private Label wlQueueURL, wlQueueName, wlAckMode, wlDeliveryMode;
	private Text wQueueURL, wQueueName, wAckMode, wDeliveryMode;

	private FormData fdlQueueURL, fdlQueueName, fdlAckMode, fdlDeliveryMode, fdQueueURL, fdQueueName, fdAckMode,
			fdDeliveryMode;

	private Text wValue;

	private ColumnInfo[] colinf;
	private TableView wFields;
	private FormData fdFields;
	private Map<String, Integer> inputFields;
	private Map<Integer, String> acknowlegeModes;
	private Map<Integer, String> deliveryModes;

	public ActiveMQProducerDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		input = (ActiveMQProducerMeta) in;
		inputFields = new HashMap<String, Integer>();
		acknowlegeModes = new HashMap<Integer, String>();
		deliveryModes = new HashMap<Integer, String>();
	}

	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("QueuePluginDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(Messages.getString("QueuePluginDialog.StepName.Label")); //$NON-NLS-1$
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wTabFolder = new CTabFolder(shell, SWT.BORDER);
		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
		wTabFolder.setSimple(false);

		// Content tab starts ***

		this.wContentTab = new CTabItem(wTabFolder, SWT.NONE);
		this.wContentTab.setText(Messages.getString("QueuePluginDialog.ContentTab.TabTitle"));

		FormLayout contentLayout = new FormLayout();
		contentLayout.marginWidth = Const.FORM_MARGIN;
		contentLayout.marginHeight = Const.FORM_MARGIN;

		Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wContentComp);
		wContentComp.setLayout(contentLayout);

		// Queue URL
		wlQueueURL = new Label(wContentComp, SWT.RIGHT);
		wlQueueURL.setText(Messages.getString("QueuePluginDialog.queueURL.Label"));
		props.setLook(wlQueueURL);
		fdlQueueURL = new FormData();
		fdlQueueURL.left = new FormAttachment(0, 0);
		fdlQueueURL.right = new FormAttachment(middle, -margin);
		fdlQueueURL.top = new FormAttachment(0, margin);
		wlQueueURL.setLayoutData(fdlQueueURL);
		wQueueURL = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wQueueURL.setText("");
		props.setLook(wQueueURL);
		wQueueURL.addModifyListener(lsMod);
		fdQueueURL = new FormData();
		fdQueueURL.left = new FormAttachment(middle, 0);
		fdQueueURL.top = new FormAttachment(0, margin);
		fdQueueURL.right = new FormAttachment(100, 0);
		wQueueURL.setLayoutData(fdQueueURL);

		// Queue Name
		wlQueueName = new Label(wContentComp, SWT.RIGHT);
		wlQueueName.setText(Messages.getString("QueuePluginDialog.queueName.Label"));
		props.setLook(wlQueueName);
		fdlQueueName = new FormData();
		fdlQueueName.left = new FormAttachment(0, 0);
		fdlQueueName.right = new FormAttachment(middle, -margin);
		fdlQueueName.top = new FormAttachment(wQueueURL, margin);
		wlQueueName.setLayoutData(fdlQueueName);
		wQueueName = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wQueueName.setText("");
		props.setLook(wQueueName);
		wQueueName.addModifyListener(lsMod);
		fdQueueName = new FormData();
		fdQueueName.left = new FormAttachment(middle, 0);
		fdQueueName.top = new FormAttachment(wQueueURL, margin);
		fdQueueName.right = new FormAttachment(100, 0);
		wQueueName.setLayoutData(fdQueueName);

		// Acknowledgement mode
		wlAckMode = new Label(wContentComp, SWT.RIGHT);
		wlAckMode.setText(Messages.getString("QueuePluginDialog.ackMode.Label"));
		props.setLook(wlAckMode);
		fdlAckMode = new FormData();
		fdlAckMode.left = new FormAttachment(0, 0);
		fdlAckMode.right = new FormAttachment(middle, -margin);
		fdlAckMode.top = new FormAttachment(wQueueName, margin);
		wlAckMode.setLayoutData(fdlAckMode);
		wAckMode = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wAckMode.setText("");
		props.setLook(wAckMode);
		wAckMode.addModifyListener(lsMod);
		fdAckMode = new FormData();
		fdAckMode.left = new FormAttachment(middle, 0);
		fdAckMode.top = new FormAttachment(wQueueName, margin);
		fdAckMode.right = new FormAttachment(100, 0);
		wAckMode.setLayoutData(fdAckMode);

		// Delivery Mode
		wlDeliveryMode = new Label(wContentComp, SWT.RIGHT);
		wlDeliveryMode.setText(Messages.getString("QueuePluginDialog.deliveryMode.Label"));
		props.setLook(wlDeliveryMode);
		fdlDeliveryMode = new FormData();
		fdlDeliveryMode.left = new FormAttachment(0, 0);
		fdlDeliveryMode.right = new FormAttachment(middle, -margin);
		fdlDeliveryMode.top = new FormAttachment(wAckMode, margin);
		wlDeliveryMode.setLayoutData(fdlDeliveryMode);
		wDeliveryMode = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wDeliveryMode.setText("");
		props.setLook(wDeliveryMode);
		wDeliveryMode.addModifyListener(lsMod);
		fdDeliveryMode = new FormData();
		fdDeliveryMode.left = new FormAttachment(middle, 0);
		fdDeliveryMode.top = new FormAttachment(wAckMode, margin);
		fdDeliveryMode.right = new FormAttachment(100, 0);
		wDeliveryMode.setLayoutData(fdDeliveryMode);

		fdContentComp = new FormData();
		fdContentComp.left = new FormAttachment(0, 0);
		fdContentComp.top = new FormAttachment(0, 0);
		fdContentComp.right = new FormAttachment(100, 0);
		fdContentComp.bottom = new FormAttachment(100, 0);
		wContentComp.setLayoutData(fdContentComp);

		wContentComp.layout();
		wContentTab.setControl(wContentComp);

		// ///////////////////////////////////////////////////////////
		// / END OF CONTENT TAB
		// ///////////////////////////////////////////////////////////

		// Fields tab...
		//
		wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
		wFieldsTab.setText(Messages.getString("QueuePluginDialog.FieldsTab.TabTitle"));

		FormLayout fieldsLayout = new FormLayout();
		fieldsLayout.marginWidth = Const.FORM_MARGIN;
		fieldsLayout.marginHeight = Const.FORM_MARGIN;

		Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
		wFieldsComp.setLayout(fieldsLayout);
		props.setLook(wFieldsComp);

		wGet = new Button(wFieldsComp, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
		wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));

		setButtonPositions(new Button[] { wGet }, margin, null);

		final int FieldsCols = 10;
		final int FieldsRows = input.getOutputFields().length;

		String[] dats = Const.getDateFormats();

		// Prepare a list of possible formats...
		String[] nums = Const.getNumberFormats();
		int totsize = dats.length + nums.length;
		String[] formats = new String[totsize];
		for (int x = 0; x < dats.length; x++) {
			formats[x] = dats[x];
		}
		for (int x = 0; x < nums.length; x++) {
			formats[dats.length + x] = nums[x];
		}

		colinf = new ColumnInfo[FieldsCols];
		colinf[0] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.NameColumn.Column"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		colinf[1] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.TypeColumn.Column"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes());
		colinf[2] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.FormatColumn.Column"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, formats);
		colinf[3] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.LengthColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[4] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.PrecisionColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[5] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.CurrencyColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[6] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.DecimalColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[7] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.GroupColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[8] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.TrimTypeColumn.Column"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.trimTypeDesc, true);
		colinf[9] = new ColumnInfo(BaseMessages.getString(PKG, "QueuePluginDialog.NullColumn.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);

		wFields = new TableView(transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows,
				lsMod, props);

		fdFields = new FormData();
		fdFields.left = new FormAttachment(0, 0);
		fdFields.top = new FormAttachment(0, 0);
		fdFields.right = new FormAttachment(100, 0);
		fdFields.bottom = new FormAttachment(wGet, -margin);
		wFields.setLayoutData(fdFields);

		//
		// Search the fields in the background

		final Runnable runnable = new Runnable() {
			public void run() {
				StepMeta stepMeta = transMeta.findStep(stepname);
				if (stepMeta != null) {
					try {
						RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

						// Remember these fields...
						for (int i = 0; i < row.size(); i++) {
							inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
						}
						setComboBoxes();
					} catch (KettleException e) {
						logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
					}
				}
			}
		};
		new Thread(runnable).start();

		fdFieldsComp = new FormData();
		fdFieldsComp.left = new FormAttachment(0, 0);
		fdFieldsComp.top = new FormAttachment(0, 0);
		fdFieldsComp.right = new FormAttachment(100, 0);
		fdFieldsComp.bottom = new FormAttachment(100, 0);
		wFieldsComp.setLayoutData(fdFieldsComp);

		wFieldsComp.layout();
		wFieldsTab.setControl(wFieldsComp);

		fdTabFolder = new FormData();
		fdTabFolder.left = new FormAttachment(0, 0);
		fdTabFolder.top = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom = new FormAttachment(100, -50);
		wTabFolder.setLayoutData(fdTabFolder);

		// ===================End bottom button
		// follows==========================

		// Some buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(Messages.getString("System.Button.OK")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(Messages.getString("System.Button.Cancel")); //$NON-NLS-1$

		BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, wValue);

		// Add listeners
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);
		wQueueName.addSelectionListener(lsDef);
		wQueueURL.addSelectionListener(lsDef);
		wAckMode.addSelectionListener(lsDef);
		wDeliveryMode.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		lsResize = new Listener() {
			public void handleEvent(Event event) {
				Point size = shell.getSize();
				wFields.setSize(size.x - 10, size.y - 50);
				wFields.table.setSize(size.x - 10, size.y - 50);
				wFields.redraw();
			}
		};
		shell.addListener(SWT.Resize, lsResize);

		wTabFolder.setSelection(0);
		input.setChanged(changed);

		// Set the shell size, based upon previous time...
		setSize();

		getData();
		input.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}

	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	public void getData() {

		wQueueName.setText(Const.NVL(input.getQueueName(), ""));
		wQueueURL.setText(Const.NVL(input.getQueueURL(), ""));
		wAckMode.setText(Const.NVL(input.getAckMode().toString(), ""));
		wDeliveryMode.setText(Const.NVL(input.getDeliveryMode().toString(), ""));

		logDebug("getting fields info...");

		for (int i = 0; i < input.getOutputFields().length; i++) {
			QueueMessageField field = input.getOutputFields()[i];

			TableItem item = wFields.table.getItem(i);
			if (field.getName() != null) {
				item.setText(1, field.getName());
			}
			item.setText(2, field.getTypeDesc());
			if (field.getFormat() != null) {
				item.setText(3, field.getFormat());
			}
			if (field.getLength() >= 0) {
				item.setText(4, "" + field.getLength());
			}
			if (field.getPrecision() >= 0) {
				item.setText(5, "" + field.getPrecision());
			}
			if (field.getCurrencySymbol() != null) {
				item.setText(6, field.getCurrencySymbol());
			}
			if (field.getDecimalSymbol() != null) {
				item.setText(7, field.getDecimalSymbol());
			}
			if (field.getGroupingSymbol() != null) {
				item.setText(8, field.getGroupingSymbol());
			}
			String trim = field.getTrimTypeDesc();
			if (trim != null) {
				item.setText(9, trim);
			}
			if (field.getNullString() != null) {
				item.setText(10, field.getNullString());
			}
		}

		wFields.optWidth(true);

		wStepname.selectAll();
		wStepname.setFocus();
	}

	private void cancel() {
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void ok() {
		if (Utils.isEmpty(wStepname.getText())) {
			return;
		}
		stepname = wStepname.getText(); // return value
		saveInfoInMeta(input);
		dispose();
	}

	private void saveInfoInMeta(ActiveMQProducerMeta queueMeta) {
		queueMeta.setAckMode(Integer.parseInt(wAckMode.getText()));
		queueMeta.setDeliveryMode(Integer.parseInt(wDeliveryMode.getText()));
		queueMeta.setQueueName(wQueueName.getText());
		queueMeta.setQueueURL(wQueueURL.getText());

		int i;
		// Table table = wFields.table;

		int nrfields = wFields.nrNonEmpty();

		queueMeta.allocate(nrfields);

		for (i = 0; i < nrfields; i++) {
			QueueMessageField field = new QueueMessageField();

			TableItem item = wFields.getNonEmpty(i);
			field.setName(item.getText(1));
			field.setType(item.getText(2));
			field.setFormat(item.getText(3));
			field.setLength(Const.toInt(item.getText(4), -1));
			field.setPrecision(Const.toInt(item.getText(5), -1));
			field.setCurrencySymbol(item.getText(6));
			field.setDecimalSymbol(item.getText(7));
			field.setGroupingSymbol(item.getText(8));
			field.setTrimType(ValueMeta.getTrimTypeByDesc(item.getText(9)));
			field.setNullString(item.getText(10));
			queueMeta.getOutputFields()[i] = field;
		}

	}

	protected void setComboBoxes() {
		// Something was changed in the row.		//
		final Map<String, Integer> fields = new HashMap<String, Integer>();
		// Add the currentMeta fields...
		fields.putAll(inputFields);
		Set<String> keySet = fields.keySet();
		List<String> entries = new ArrayList<String>(keySet);
		String[] fieldNames = entries.toArray(new String[entries.size()]);
		Const.sortStrings(fieldNames);
		colinf[0].setComboValues(fieldNames);
	}
	
	protected void setModes(){
		deliveryModes.put(1, "NON_PERSISTENT");
		deliveryModes.put(2, "PERSISTENT");
		
		acknowlegeModes.put(1, "AUTO_ACKNOWLEDGE");
		acknowlegeModes.put(2, "CLIENT_ACKNOWLEDGE");
		acknowlegeModes.put(3, "DUPS_OK_ACKNOWLEDGE");
		acknowlegeModes.put(0, "SESSION_TRANSACTED");
	}
}
