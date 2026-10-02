import { Component, inject, OnInit } from '@angular/core';
import { Order } from '../../shared/Models/Order';
import { OrderService } from '../../core/services/order.service';
import { CurrencyPipe, DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';

@Component({
  selector: 'app-order',
  imports: [
    RouterLink,
    CurrencyPipe,
    DatePipe
  ],
  templateUrl: './order.component.html',
  styleUrl: './order.component.scss',
})
export class OrderComponent {
  private orderService = inject(OrderService);
  orders: Order[] | null = null;

  ngOnInit(): void {

    this.orderService.getOrdersForUser().subscribe({
      next: (data) => {
        console.log('API returned:', data);

        this.orders = data;
      },
      error: (error) => {
        console.error(error);
      }
    });
  }
}
