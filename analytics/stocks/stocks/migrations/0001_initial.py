# Generated by Django 2.1 on 2018-08-18 10:03

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Industry',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name='Listing',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateTimeField()),
                ('opening', models.DecimalField(decimal_places=5, max_digits=11)),
                ('high', models.DecimalField(decimal_places=5, max_digits=11)),
                ('low', models.DecimalField(decimal_places=5, max_digits=11)),
                ('closing', models.DecimalField(decimal_places=5, max_digits=11)),
                ('wap', models.DecimalField(decimal_places=11, max_digits=20)),
                ('traded', models.BigIntegerField()),
                ('trades', models.BigIntegerField()),
                ('turnover', models.BigIntegerField()),
                ('deliverable', models.BigIntegerField()),
                ('ratio', models.DecimalField(decimal_places=2, max_digits=10)),
                ('spread_high_low', models.DecimalField(decimal_places=10, max_digits=20)),
                ('spread_close_open', models.DecimalField(decimal_places=10, max_digits=20)),
            ],
        ),
        migrations.CreateModel(
            name='Stock',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('security', models.BigIntegerField()),
                ('sid', models.CharField(max_length=15)),
                ('name', models.CharField(max_length=255)),
                ('group', models.CharField(blank=True, default='', max_length=5)),
                ('face_value', models.DecimalField(decimal_places=4, max_digits=10)),
                ('isin', models.CharField(max_length=15)),
                ('industry', models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.SET_NULL, to='stocks.Industry')),
            ],
        ),
        migrations.AddField(
            model_name='listing',
            name='stock',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='stocks.Stock'),
        ),
    ]