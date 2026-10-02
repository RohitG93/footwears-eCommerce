import { CurrencyPipe, DatePipe } from '@angular/common';
import { Component, inject } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { PaymentCardPipe } from '../../../shared/pipes/payment-card-pipe';
import { AddressPipePipe } from '../../../shared/pipes/address-pipe-pipe';
import { OrderService } from '../../../core/services/order.service';
import { ActivatedRoute, Router } from '@angular/router';
import { Order } from '../../../shared/Models/Order';

@Component({
  selector: 'app-order-detail',
  imports: [
    MatCardModule,
    DatePipe,
    MatButton,
    AddressPipePipe,  
    PaymentCardPipe,
    CurrencyPipe
  ],
  templateUrl: './order-detail.component.html',
  styleUrl: './order-detail.component.scss',
})
export class OrderDetailComponent {
  private activatedRoute = inject(ActivatedRoute);
  private router = inject(Router);
  private orderService = inject(OrderService);
  order?: Order;
  buttonText = 'Return to orders'

  ngOnInit(): void {
    this.loadOrder();
  }

  onReturnClick() {
      this.router.navigateByUrl('/orders')
  }

  loadOrder() {
    const id = this.activatedRoute.snapshot.paramMap.get('id');
    if (!id) return;

    this.orderService.getOrderDetailed(+id).subscribe({
      next: order => this.order = order
    })
  }
}
