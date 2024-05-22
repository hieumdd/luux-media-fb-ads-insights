import Joi, { Schema } from 'joi';
import { TableField } from '@google-cloud/bigquery';

import { dayjs } from '../dayjs';

export type Field = {
    validationSchema: { [key: string]: Schema };
    tableSchema: TableField;
};
type FieldType = { validationSchema: Schema; tableSchema: string };

abstract class BaseField implements Field {
    name: string;
    fieldType: FieldType;
    array: boolean;

    constructor(name: string, fieldType: FieldType, array = false) {
        this.name = name;
        this.fieldType = fieldType;
        this.array = array;
    }

    get validationSchema() {
        return {
            [this.name]: this.array
                ? Joi.array().items(this.fieldType.validationSchema)
                : this.fieldType.validationSchema,
        };
    }
    get tableSchema() {
        return { name: this.name, type: this.fieldType.tableSchema, mode: this.array ? 'REPEATED' : 'NULLABLE' };
    }
}

export class StringField extends BaseField implements Field {
    constructor(name: string, array = false) {
        super(name, { validationSchema: Joi.string(), tableSchema: 'STRING' }, array);
    }
}

export class IDField extends BaseField implements Field {
    constructor(name: string, array = false) {
        super(name, { validationSchema: Joi.string(), tableSchema: 'INT64' }, array);
    }
}

export class NumericField extends BaseField implements Field {
    constructor(name: string, array = false) {
        super(name, { validationSchema: Joi.number(), tableSchema: 'NUMERIC' }, array);
    }
}

export class DateField extends BaseField implements Field {
    constructor(name: string, array = false) {
        super(
            name,
            {
                validationSchema: Joi.custom((value: string | undefined) => {
                    if (!value) {
                        return null;
                    }
                    const parsedDate = dayjs(value);
                    return parsedDate.isValid() ? parsedDate.format('YYYY-MM-DD') : null;
                }),
                tableSchema: 'DATE',
            },
            array,
        );
    }
}

export class TimestampField extends BaseField implements Field {
    constructor(name: string, array = false) {
        super(
            name,
            {
                validationSchema: Joi.custom((value: string | undefined) => {
                    if (!value) {
                        return null;
                    }
                    const parsedDate = dayjs(value);
                    return parsedDate.isValid() ? parsedDate.toDate() : null;
                }),
                tableSchema: 'TIMESTAMP',
            },
            array,
        );
    }
}

export class RecordField implements Field {
    name: string;
    fields: Field[];
    array: boolean;

    constructor(name: string, fields: Field[], array = false) {
        this.name = name;
        this.fields = fields;
        this.array = array;
    }

    get validationSchema() {
        const recordSchema = Joi.object(Object.assign({}, ...this.fields.map((field) => field.validationSchema)));
        return {
            [this.name]: this.array ? Joi.array().items(recordSchema) : recordSchema,
        };
    }
    get tableSchema() {
        return {
            name: this.name,
            type: 'RECORD',
            mode: this.array ? 'REPEATED' : 'NULLABLE',
            fields: this.fields.map((field) => field.tableSchema),
        };
    }
}
