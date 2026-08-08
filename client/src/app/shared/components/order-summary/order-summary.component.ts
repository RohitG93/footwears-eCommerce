import { Component, inject } from '@angular/core';
import { CartService } from '../../../core/services/cart.service';
import { MatIcon } from '@angular/material/icon';
import { CurrencyPipe } from '@angular/common';
import { MatFormField, MatLabel } from '@angular/material/select';
import { MatButton } from '@angular/material/button';
import { FormsModule } from '@angular/forms';
import { MatInput } from '@angular/material/input';
import { RouterLink } from '@angular/router';

@Component({
  selector: 'app-order-summary',
  imports: [
    MatFormField,
    MatLabel,
    MatButton,
    RouterLink,
    MatInput, 
    CurrencyPipe,
    FormsModule,
    MatIcon
  ],
  templateUrl: './order-summary.component.html',
  styleUrl: './order-summary.component.scss',
})
export class OrderSummaryComponent {
  cartService = inject(CartService)
}
