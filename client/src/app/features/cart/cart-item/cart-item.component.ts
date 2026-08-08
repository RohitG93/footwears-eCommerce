import { Component, inject, input } from '@angular/core';
import { CartItem } from '../../../shared/Models/cart';
import { RouterLink } from '@angular/router';
import { MatIcon } from '@angular/material/icon';
import { CurrencyPipe } from '@angular/common';
import { CartService } from '../../../core/services/cart.service';

@Component({
  selector: 'app-cart-item',
  imports: [RouterLink,
    MatIcon,
    CurrencyPipe
  ],
  templateUrl: './cart-item.component.html',
  styleUrl: './cart-item.component.scss',
})
export class CartItemComponent {
  item = input.required<CartItem>();
  cartService = inject(CartService)

  incrementQuantity() {
    this.cartService.addItemsToCart(this.item());
  }

  decrementQuantity() {
    this.cartService.removeItemFromCart(this.item().productId, 1);
  }

  removeItemFromCart() {
    this.cartService.removeItemFromCart(this.item().productId, this.item().quantity);
  }
}
